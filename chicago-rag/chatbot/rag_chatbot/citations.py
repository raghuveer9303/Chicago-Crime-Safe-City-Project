"""Build structured citations (separate from narrative answer) from LangGraph messages."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import ToolMessage

from rag_chatbot.schemas import CitationItem


def _parse_langsearch_tool_content(content: str) -> list[CitationItem]:
    out: list[CitationItem] = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return out
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, dict):
            continue
        url = row.get("url")
        if not url:
            continue
        out.append(
            CitationItem(
                source="web",
                url=str(url),
                title=row.get("name") or row.get("title"),
                detail=(row.get("snippet") or row.get("summary") or "")[:2000],
            )
        )
    return out


def _tool_name(name: str | None) -> str:
    return (name or "").lower()


def _parse_graph_citation(name: str, content: str) -> CitationItem | None:
    """Create a graph citation entry from Neo4j-backed tool outputs."""
    if "read_neo4j_cypher" not in name and "neo4j" not in name:
        return None
    detail = content.strip()
    if not detail:
        detail = "Queried the Chicago crime knowledge graph."
    else:
        detail = detail[:2000]
    return CitationItem(
        source="graph",
        title="Chicago Crime Knowledge Graph",
        detail=detail,
    )


def extract_citations_from_messages(messages: list[Any]) -> list[CitationItem]:
    """Collect web + graph citations from tool results."""
    citations: list[CitationItem] = []
    seen_urls: set[str] = set()
    seen_graph_details: set[str] = set()

    for m in messages:
        if isinstance(m, ToolMessage):
            name = _tool_name(getattr(m, "name", None))
            content = m.content if isinstance(m.content, str) else str(m.content)
            is_langsearch = "langsearch" in name or "web_search" in name
            if not is_langsearch and content.strip().startswith("["):
                is_langsearch = '"url"' in content
            if is_langsearch:
                for c in _parse_langsearch_tool_content(content):
                    if c.url and c.url not in seen_urls:
                        seen_urls.add(c.url)
                        citations.append(c)
            graph_citation = _parse_graph_citation(name, content)
            if graph_citation is not None:
                detail_key = graph_citation.detail or ""
                if detail_key not in seen_graph_details:
                    seen_graph_details.add(detail_key)
                    citations.append(graph_citation)

    return citations
