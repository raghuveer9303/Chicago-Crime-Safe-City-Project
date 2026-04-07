"""Cypher guardrails for Neo4j read tools (direct Bolt or MCP)."""

import re
from typing import Final

_READ_FORBIDDEN = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP)\b|\bcall\s+db\.",
    re.IGNORECASE,
)

# Matches SQL-style line comments: -- ... (to end of line)
_LINE_COMMENT: Final[re.Pattern] = re.compile(r"--[^\n]*")

# Matches SQL-style block comments: /* ... */
_BLOCK_COMMENT: Final[re.Pattern] = re.compile(r"/\*.*?\*/", re.DOTALL)

# Matches `WHEN NOT <bare_identifier> THEN` (must check before the non-NOT version)
_BARE_WHEN_NOT: Final[re.Pattern] = re.compile(
    r"\bWHEN\s+NOT\s+([\w]+(?:\.[\w]+)*)\s+THEN\b", re.IGNORECASE
)
# Matches `WHEN <bare_identifier> THEN` (no comparison operator between WHEN and THEN)
_BARE_WHEN: Final[re.Pattern] = re.compile(
    r"\bWHEN\s+([\w]+(?:\.[\w]+)*)\s+THEN\b", re.IGNORECASE
)

_BROKEN_STRING_AFTER_OP = re.compile(
    # Repairs a common LLM mistake: CONTAINS 'O'Hare'  -> CONTAINS 'O\'Hare'
    # Trigger only when a quote closes early, then continues with a word, then closes later.
    r"(?P<op>\bCONTAINS\b|\bSTARTS\s+WITH\b|\bENDS\s+WITH\b)\s*'(?P<a>[^']*)'(?P<b>[A-Za-z][^']*)'",
    re.IGNORECASE,
)


def repair_common_unescaped_apostrophes(query: str) -> str:
    """
    Best-effort repair for a frequent Cypher syntax error produced by LLMs:
    an apostrophe inside a single-quoted string literal isn't escaped (e.g. O'Hare).

    Intentionally conservative: only targets CONTAINS/STARTS WITH/ENDS WITH patterns where
    the query has clearly broken quoting.
    """
    q = query
    for _ in range(5):
        new_q = _BROKEN_STRING_AFTER_OP.sub(
            lambda m: f"{m.group('op')} '{m.group('a')}\\'{m.group('b')}'",
            q,
        )
        if new_q == q:
            break
        q = new_q
    return q


def strip_cypher_comments(query: str) -> str:
    """Remove SQL-style comments that LLMs sometimes add to Cypher.

    Cypher does NOT support -- or /* */ comments.
    In Cypher, -- is an undirected relationship pattern, so leaving these
    in causes confusing syntax errors.
    """
    q = _BLOCK_COMMENT.sub("", query)
    q = _LINE_COMMENT.sub("", q)
    # Collapse any leftover blank lines from stripped comments
    q = re.sub(r"\n\s*\n", "\n", q).strip()
    return q


def repair_bare_flag_predicates(query: str) -> str:
    """Fix bare identifiers used as boolean predicates in CASE WHEN.

    LLMs often generate ``CASE WHEN flag THEN …`` but the *_flag properties
    in this schema are stored as integers (Long 0/1), not booleans.
    Neo4j raises TypeError when an integer is used where a predicate is expected.

    Rewrites:
      WHEN violent THEN        →  WHEN violent = 1 THEN
      WHEN NOT violent THEN    →  WHEN violent <> 1 THEN
    """
    # Process NOT variant first (more specific pattern)
    q = _BARE_WHEN_NOT.sub(r"WHEN \1 <> 1 THEN", query)
    q = _BARE_WHEN.sub(r"WHEN \1 = 1 THEN", q)
    return q


def validate_read_only_cypher(query: str) -> None:
    q = query.strip()
    if not q:
        raise ValueError("Empty Cypher query.")
    if _READ_FORBIDDEN.search(q):
        raise ValueError(
            "Only read-only Cypher is allowed (no CREATE/MERGE/DELETE/SET/DROP/etc.)."
        )
