"""Streaming variant of the Chicago RAG agent using LangGraph astream_events (v2)."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import AsyncIterator
from zoneinfo import ZoneInfo

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_mistralai import ChatMistralAI
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.prebuilt import create_react_agent

from rag_chatbot.citations import extract_citations_from_messages
from rag_chatbot.config import Settings
from rag_chatbot.prompts import ROUTER_SYSTEM_PROMPT, SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Monkey-patch: fix Mistral 422 on tool messages with list content
# ---------------------------------------------------------------------------
# The current langchain-mistralai serialiser passes ToolMessage.content as-is.
# When LangGraph / MCP tools return content as a list of content-block dicts
#   e.g.  [{"type": "text", "text": "[]", "id": "lc_..."}]
# the Mistral API rejects it with HTTP 422 ("Input should be a valid string").
# This patch normalises that list into a plain string before the request is
# built, so tool results always reach Mistral in the format it expects.
# ---------------------------------------------------------------------------
import langchain_mistralai.chat_models as _mistral_cm  # noqa: E402

_original_convert = _mistral_cm._convert_message_to_mistral_chat_message


def _patched_convert(message):  # type: ignore[override]
    """Wrap the original converter, flattening ToolMessage.content to str."""
    if isinstance(message, ToolMessage) and isinstance(message.content, list):
        # Extract text from content blocks and join into a single string
        parts: list[str] = []
        for block in message.content:
            if isinstance(block, dict):
                parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        message = message.model_copy(update={"content": "".join(parts)})
    return _original_convert(message)


# Apply the patch once at import time
_mistral_cm._convert_message_to_mistral_chat_message = _patched_convert
from rag_chatbot.schemas import (
    CitationItem,
    TaskComplexity,
    WsDoneEvent,
    WsErrorEvent,
    WsThinkingEvent,
    WsTokenEvent,
    WsToolCallEvent,
    WsToolResultEvent,
)

logger = logging.getLogger(__name__)
_RETRY_LATER_MSG = "Something went wrong on our side. Please try again after a couple of minutes."
_INTERNAL_ERROR_PATTERN = re.compile(
    r"(neo4j|cypher|gql_status|clienterror|syntaxerror|database|read_neo4j_cypher|traceback|exception)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Human-readable tool labels shown in the frontend activity banners
# ---------------------------------------------------------------------------
_TOOL_LABELS: dict[str, str] = {
    "read_neo4j_cypher": "🔍 Looking up crime data…",
    "get_neo4j_schema": "🗂️ Checking data structure…",
    "langsearch_web_search": "🌐 Searching the web…",
}


def _tool_label(raw_name: str) -> str:
    for key, label in _TOOL_LABELS.items():
        if key in raw_name.lower():
            return label
    return f"⚙️ Running {raw_name}…"


def _brief_result(content) -> str:
    """Return a short, human-readable summary of a tool result (≤ 120 chars)."""
    if isinstance(content, list):
        # List of blocks — grab first text block
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                content = block.get("text", "")
                break
        else:
            content = str(content)
    text = str(content).strip()
    if not text:
        return "No results returned."
    if _INTERNAL_ERROR_PATTERN.search(text):
        return _RETRY_LATER_MSG
    # Try to give a count if it looks like JSON array
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return f"Found {len(parsed)} result(s)."
    except (json.JSONDecodeError, ValueError):
        pass
    return text[:117] + "…" if len(text) > 120 else text


def _runtime_system_prompt() -> str:
    """
    Inject today's date at runtime so relative time references stay accurate.
    """
    try:
        today = datetime.now(ZoneInfo("America/Chicago")).date().isoformat()
        tz_name = "America/Chicago"
    except Exception:
        today = datetime.utcnow().date().isoformat()
        tz_name = "UTC"
    return (
        f"Today's date is {today} ({tz_name}). "
        "Treat this as the current date when interpreting user time references.\n\n"
        f"{SYSTEM_PROMPT}"
    )


# ---------------------------------------------------------------------------
# Router (unchanged from original)
# ---------------------------------------------------------------------------

def _require_mistral_api_key(settings: Settings) -> str:
    api_key = (settings.mistral_api_key or "").strip()
    if not api_key:
        raise ValueError("MISTRAL_API_KEY is required (set env var or Vault secret).")
    return api_key


async def route_with_llm_20b(settings: Settings, question: str) -> tuple[str, str]:
    """Route main agent model using structured output on the small (20B) model."""
    api_key = _require_mistral_api_key(settings)
    llm = ChatMistralAI(
        model=settings.mistral_model_small,
        api_key=api_key,
        temperature=0.0,
    )
    structured = llm.with_structured_output(TaskComplexity, method="json_schema")
    out = await structured.ainvoke(
        [
            SystemMessage(content=ROUTER_SYSTEM_PROMPT),
            HumanMessage(content=question),
        ]
    )
    if out.tier == "complex":
        return settings.mistral_model_large, "complex"
    return settings.mistral_model_medium, "simple"


# ---------------------------------------------------------------------------
# Original non-streaming invocation (kept for POST /v1/chat)
# ---------------------------------------------------------------------------

def _last_ai_text(messages: list) -> str:
    for m in reversed(messages):
        if isinstance(m, AIMessage):
            content = m.content
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        parts.append(block.get("text", ""))
                    elif isinstance(block, str):
                        parts.append(block)
                return "\n".join(parts)
            return str(content)
    return ""


async def _repair_incomplete_tool_history(graph, cfg: dict, thread_id: str) -> None:
    """
    Ensure every historical AI tool_call has a corresponding ToolMessage.

    Interrupted runs can leave checkpointed state with pending tool calls; many
    model providers reject this chat history on the next invocation.
    """
    try:
        state = await graph.aget_state(cfg)
    except Exception:
        logger.exception("Could not inspect graph state for thread_id=%s", thread_id)
        return

    values = getattr(state, "values", {}) or {}
    messages = values.get("messages") or []
    if not isinstance(messages, list) or not messages:
        return

    outstanding_calls: dict[str, str] = {}
    for message in messages:
        if isinstance(message, AIMessage):
            for tc in message.tool_calls or []:
                tc_id = tc.get("id")
                if tc_id:
                    outstanding_calls[tc_id] = tc.get("name", "unknown_tool")
        elif isinstance(message, ToolMessage):
            tc_id = getattr(message, "tool_call_id", None)
            if tc_id:
                outstanding_calls.pop(tc_id, None)

    if not outstanding_calls:
        return

    recovered = [
        ToolMessage(
            tool_call_id=tc_id,
            name=tool_name,
            content=(
                "Tool execution was interrupted before a result was recorded. "
                "Continuing conversation with this tool call marked as unavailable."
            ),
        )
        for tc_id, tool_name in outstanding_calls.items()
    ]
    try:
        await graph.aupdate_state(cfg, {"messages": recovered})
        logger.warning(
            "Repaired %d dangling tool call(s) for thread_id=%s",
            len(recovered),
            thread_id,
        )
    except Exception:
        logger.exception("Failed repairing tool history for thread_id=%s", thread_id)


async def run_chicago_agent(
    settings: Settings,
    tools: list,
    checkpointer: BaseCheckpointSaver,
    question: str,
    thread_id: str,
    callbacks: list | None = None,
) -> tuple[str, str, str, list[CitationItem]]:
    """Returns (answer, main_model_name, router_tier, citations)."""
    main_model, tier = await route_with_llm_20b(settings, question)
    api_key = _require_mistral_api_key(settings)
    llm = ChatMistralAI(
        model=main_model,
        api_key=api_key,
        temperature=0.1,
    )
    graph = create_react_agent(
        llm,
        tools,
        prompt=_runtime_system_prompt(),
        checkpointer=checkpointer,
    )
    cfg: dict = {"configurable": {"thread_id": thread_id}}
    if callbacks:
        cfg["callbacks"] = callbacks
    await _repair_incomplete_tool_history(graph, cfg, thread_id)
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content=question)]},
        config=cfg,
    )
    messages = result.get("messages") or []
    answer = _last_ai_text(messages)
    citations = extract_citations_from_messages(messages)
    return answer, main_model, tier, citations


# ---------------------------------------------------------------------------
# Streaming agent — yields WsEvent objects
# ---------------------------------------------------------------------------

async def stream_chicago_agent(
    settings: Settings,
    tools: list,
    checkpointer: BaseCheckpointSaver,
    question: str,
    thread_id: str,
    callbacks: list | None = None,
) -> AsyncIterator[
    WsThinkingEvent
    | WsToolCallEvent
    | WsToolResultEvent
    | WsTokenEvent
    | WsDoneEvent
    | WsErrorEvent
]:
    """
    Async generator that yields typed WebSocket events as the agent runs.

    Event ordering:
      thinking? → (tool_call → tool_result)* → token* → done
                                            OR → error
    """
    try:
        main_model, tier = await route_with_llm_20b(settings, question)
    except Exception as exc:
        logger.exception("Routing failed for thread_id=%s", thread_id)
        yield WsErrorEvent(detail=_RETRY_LATER_MSG)
        return

    api_key = _require_mistral_api_key(settings)
    llm = ChatMistralAI(
        model=main_model,
        api_key=api_key,
        temperature=0.1,
        streaming=True,
    )
    graph = create_react_agent(
        llm,
        tools,
        prompt=_runtime_system_prompt(),
        checkpointer=checkpointer,
    )
    cfg: dict = {"configurable": {"thread_id": thread_id}}
    if callbacks:
        cfg["callbacks"] = callbacks
    await _repair_incomplete_tool_history(graph, cfg, thread_id)

    # Accumulate full answer text for the done event
    answer_parts: list[str] = []
    # Collect all messages for citation extraction at the end
    all_messages: list = []

    try:
        async for event in graph.astream_events(
            {"messages": [HumanMessage(content=question)]},
            config=cfg,
            version="v2",
        ):
            kind = event.get("event", "")
            name = event.get("name", "")
            data = event.get("data", {})

            # -------- thinking: agent chain starting --------
            if kind == "on_chain_start" and name == "LangGraph":
                yield WsThinkingEvent(text="Analysing your question…")

            # -------- token streaming --------
            elif kind == "on_chat_model_stream":
                chunk = data.get("chunk")
                if chunk is None:
                    continue
                # chunk is an AIMessageChunk
                content = chunk.content
                if isinstance(content, str) and content:
                    answer_parts.append(content)
                    yield WsTokenEvent(text=content)
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text = block.get("text", "")
                            if text:
                                answer_parts.append(text)
                                yield WsTokenEvent(text=text)

            # -------- tool call starting --------
            elif kind == "on_tool_start":
                raw_input = data.get("input") or {}
                input_summary: str | None = None
                if isinstance(raw_input, dict):
                    # Grab the query/question field if present
                    q = raw_input.get("query") or raw_input.get("question") or raw_input.get("cypher")
                    if q:
                        s = str(q)
                        input_summary = s[:80] + "…" if len(s) > 80 else s
                yield WsToolCallEvent(
                    tool=name,
                    label=_tool_label(name),
                    input_summary=input_summary,
                )

            # -------- tool call finished --------
            elif kind == "on_tool_end":
                output = data.get("output")
                result_summary: str | None = None
                if output is not None:
                    cnt = getattr(output, "content", None) or output
                    result_summary = _brief_result(cnt)
                yield WsToolResultEvent(
                    tool=name,
                    label=_tool_label(name),
                    result_summary=result_summary,
                )

            # -------- collect final messages for citations --------
            elif kind == "on_chain_end" and name == "LangGraph":
                output_data = data.get("output") or {}
                msgs = output_data.get("messages") or []
                all_messages.extend(msgs)

    except Exception as exc:
        logger.exception("stream_chicago_agent failed thread_id=%s", thread_id)
        yield WsErrorEvent(detail=_RETRY_LATER_MSG)
        return

    citations = extract_citations_from_messages(all_messages)
    full_answer = "".join(answer_parts)

    yield WsDoneEvent(
        answer=full_answer,
        citations=citations,
        thread_id=thread_id,
        model=main_model,
        router_tier=tier,
    )
