"""Load Neo4j MCP tools over HTTP with optional Cypher guardrail interceptor."""

from __future__ import annotations

import httpx
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.interceptors import MCPToolCallRequest

from rag_chatbot.guardrails import (
    repair_bare_flag_predicates,
    repair_common_unescaped_apostrophes,
    strip_cypher_comments,
    validate_read_only_cypher,
)

_MAX_NEO4J_RETRIES = 2


class CypherGuardrailInterceptor:
    """Block non-read Cypher before MCP executes read_neo4j_cypher."""

    def __init__(self) -> None:
        self._fallback_retry_count = 0

    def _get_retry_count(self, runtime: object | None) -> int:
        if runtime is not None:
            return int(getattr(runtime, "_neo4j_retry_count", 0))
        return self._fallback_retry_count

    def _set_retry_count(self, runtime: object | None, count: int) -> None:
        if runtime is not None:
            setattr(runtime, "_neo4j_retry_count", count)
        else:
            self._fallback_retry_count = count

    async def __call__(self, request: MCPToolCallRequest, handler):
        name = request.name.lower()
        is_read_neo4j = "read_neo4j_cypher" in name or name.endswith("read_neo4j_cypher")
        try:
            if is_read_neo4j:
                args = request.args or {}
                q = str(args.get("query") or "")
                q = strip_cypher_comments(q)
                q = repair_common_unescaped_apostrophes(q)
                q = repair_bare_flag_predicates(q)
                args["query"] = q
                validate_read_only_cypher(q)
            if "write_neo4j_cypher" in name:
                raise ValueError("Write Cypher is disabled by application policy.")
            result = await handler(request)
            if is_read_neo4j:
                # Successful read resets retry budget for subsequent calls.
                self._set_retry_count(request.runtime, 0)
            return result
        except Exception as exc:
            if not is_read_neo4j:
                raise

            err = str(exc).strip() or exc.__class__.__name__
            if len(err) > 1000:
                err = err[:1000] + "..."

            retries_used = self._get_retry_count(request.runtime)
            if retries_used >= _MAX_NEO4J_RETRIES:
                raise ValueError(
                    f"Neo4j tool failed after {_MAX_NEO4J_RETRIES} retry attempts. Last error: {err}"
                ) from exc

            retries_used += 1
            self._set_retry_count(request.runtime, retries_used)
            retries_left = _MAX_NEO4J_RETRIES - retries_used

            # Return tool error details to the LLM so it can repair and retry,
            # but enforce a strict retry cap.
            return (
                "NEO4J_TOOL_ERROR\n"
                f"tool={request.name}\n"
                f"message={err}\n"
                f"retries_used={retries_used}\n"
                f"retries_left={retries_left}\n"
                "Fix the query/arguments using this error and call the tool again."
            )


async def load_neo4j_mcp_tools(
    mcp_url: str,
    *,
    basic_user: str | None = None,
    basic_password: str | None = None,
) -> list:
    """Return LangChain tools from the Neo4j MCP HTTP server (streamable HTTP transport)."""
    auth: httpx.Auth | None = None
    if basic_user is not None and basic_password is not None:
        auth = httpx.BasicAuth(basic_user, basic_password)

    conn: dict = {
        "transport": "http",
        "url": mcp_url,
    }
    if auth is not None:
        conn["auth"] = auth

    client = MultiServerMCPClient(
        {"neo4j": conn},
        tool_interceptors=[CypherGuardrailInterceptor()],
        tool_name_prefix=False,
    )
    return await client.get_tools(server_name="neo4j")
