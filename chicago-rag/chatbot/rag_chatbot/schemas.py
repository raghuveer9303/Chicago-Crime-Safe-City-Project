from typing import Annotated, Literal

from pydantic import BaseModel, Field


class CitationItem(BaseModel):
    """Single citation for parsing separately from the answer text."""

    source: Literal["web", "graph"]
    url: str | None = None
    title: str | None = None
    detail: str | None = Field(
        None,
        description="Snippet, summary, or Cypher query reference for graph-backed facts.",
    )


class TaskComplexity(BaseModel):
    tier: Literal["simple", "complex"]


class ChatResponse(BaseModel):
    answer: str
    citations: list[CitationItem]
    model: str
    router_tier: Literal["simple", "complex"]
    thread_id: str


# ---------------------------------------------------------------------------
# WebSocket streaming event models
# ---------------------------------------------------------------------------


class WsThinkingEvent(BaseModel):
    """Agent is reasoning / planning (emitted before tool calls)."""

    type: Literal["thinking"] = "thinking"
    text: str


class WsToolCallEvent(BaseModel):
    """A tool is being invoked; shown as an activity banner in the UI."""

    type: Literal["tool_call"] = "tool_call"
    tool: str  # raw tool name
    label: str  # human-readable label, e.g. "🔍 Looking up crime data…"
    input_summary: str | None = None  # brief description of the input


class WsToolResultEvent(BaseModel):
    """A tool finished; briefly shown then dismissed."""

    type: Literal["tool_result"] = "tool_result"
    tool: str
    label: str
    result_summary: str | None = None  # first ~120 chars of result


class WsTokenEvent(BaseModel):
    """Incremental answer token — append to the answer text in the UI."""

    type: Literal["token"] = "token"
    text: str


class WsDoneEvent(BaseModel):
    """Terminal event: full answer + citations.  Close connection after this."""

    type: Literal["done"] = "done"
    answer: str
    citations: list[CitationItem]
    thread_id: str
    model: str
    router_tier: Literal["simple", "complex"]


class WsErrorEvent(BaseModel):
    """Something went wrong. Connection will be closed after this."""

    type: Literal["error"] = "error"
    detail: str


# Discriminated union — use event["type"] to pick the right model on the client.
WsEvent = Annotated[
    WsThinkingEvent
    | WsToolCallEvent
    | WsToolResultEvent
    | WsTokenEvent
    | WsDoneEvent
    | WsErrorEvent,
    Field(discriminator="type"),
]
