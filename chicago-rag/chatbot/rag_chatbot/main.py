import asyncio
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from rag_chatbot.config import Settings, get_settings
from rag_chatbot.graph_workflow import run_chicago_agent, stream_chicago_agent
from rag_chatbot.mcp_client import load_neo4j_mcp_tools
from rag_chatbot.schemas import (
    ChatResponse,
    WsDoneEvent,
    WsErrorEvent,
    WsThinkingEvent,
    WsTokenEvent,
    WsToolCallEvent,
    WsToolResultEvent,
)
from rag_chatbot.tools import make_langsearch_tool

logger = logging.getLogger(__name__)
_RETRY_LATER_MSG = "Something went wrong on our side. Please try again after a couple of minutes."
_DB_ERROR_PATTERN = re.compile(
    r"(neo4j|cypher|gql_status|clienterror|syntaxerror|database|read_neo4j_cypher)",
    re.IGNORECASE,
)


def _public_error_detail(exc: Exception) -> str:
    """Return a safe user-facing error message without internal details."""
    text = str(exc).strip()
    if not text:
        return _RETRY_LATER_MSG
    if _DB_ERROR_PATTERN.search(text):
        return _RETRY_LATER_MSG
    # Keep behavior consistent and avoid exposing internals for all unexpected errors.
    return _RETRY_LATER_MSG

# Startup readiness flag — set to True once background tool-loading completes.
_startup_complete = False
_startup_error: str | None = None


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=16000)
    session_id: str | None = Field(
        None,
        max_length=256,
        description="Thread id for LangGraph checkpointing; omit to start a new thread.",
    )


# ---------------------------------------------------------------------------
# Langfuse helpers (unchanged)
# ---------------------------------------------------------------------------

def _apply_langfuse_env(settings: Settings) -> None:
    if not settings.langfuse_public_key or not settings.langfuse_secret_key:
        return
    os.environ["LANGFUSE_HOST"] = settings.langfuse_host.rstrip("/")
    os.environ["LANGFUSE_PUBLIC_KEY"] = settings.langfuse_public_key
    os.environ["LANGFUSE_SECRET_KEY"] = settings.langfuse_secret_key


def _langfuse_callbacks(settings: Settings):
    if not settings.langfuse_langchain_callbacks:
        return None
    _apply_langfuse_env(settings)
    if not settings.langfuse_public_key or not settings.langfuse_secret_key:
        return None
    from langfuse.langchain import CallbackHandler

    return [CallbackHandler(update_trace=True)]


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_complete, _startup_error

    settings = get_settings()
    os.environ["OTEL_SERVICE_NAME"] = settings.otel_service_name
    _apply_langfuse_env(settings)

    # Set up checkpointer synchronously (in-memory is instant; Postgres is fast)
    app.state.checkpoint_pool = None
    app.state.tools = []  # empty until background task completes
    if settings.checkpoint_postgres_uri:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(
            conninfo=settings.checkpoint_postgres_uri,
            min_size=1,
            max_size=10,
            kwargs={"autocommit": True},
            open=False,
        )
        await pool.open()
        cp = AsyncPostgresSaver(pool)
        await cp.setup()
        app.state.checkpointer = cp
        app.state.checkpoint_pool = pool
    else:
        from langgraph.checkpoint.memory import InMemorySaver

        app.state.checkpointer = InMemorySaver()

    # Load MCP tools in a background task so uvicorn starts serving /health
    # immediately — the Docker health check won't fail during slow MCP init.
    async def _load_tools() -> None:
        global _startup_complete, _startup_error
        try:
            mcp_user = settings.neo4j_mcp_http_user or settings.neo4j_username
            mcp_pass = settings.neo4j_mcp_http_password or settings.neo4j_password
            neo4j_tools = await load_neo4j_mcp_tools(
                settings.neo4j_mcp_url,
                basic_user=mcp_user or None,
                basic_password=mcp_pass or None,
            )
            langsearch = make_langsearch_tool(settings.langsearch_api_key)
            app.state.tools = [*neo4j_tools, langsearch]
            logger.info("Startup complete: %d tools loaded.", len(app.state.tools))
            _startup_complete = True
        except Exception as exc:
            _startup_error = str(exc)
            logger.error("Background tool loading failed: %s", exc)
            # Mark startup complete so /health/ready returns 503 with detail
            # rather than hanging indefinitely.
            _startup_complete = True

    _bg_task = asyncio.create_task(_load_tools())

    yield

    _bg_task.cancel()
    if app.state.checkpoint_pool is not None:
        await app.state.checkpoint_pool.close()


# ---------------------------------------------------------------------------
# App + CORS / allowed-origins middleware
# ---------------------------------------------------------------------------

app = FastAPI(title="Chicago Crime RAG", lifespan=lifespan)

# Allowed origin list for the WebSocket endpoint.
# Set ALLOWED_ORIGINS in .env as a comma-separated list, e.g.:
#   ALLOWED_ORIGINS=http://localhost:5173,https://mycrimefrontend.example.com
# Leave blank / "*" to allow all origins during development.
_RAW_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS: list[str] = (
    ["*"]
    if _RAW_ORIGINS.strip() == "*"
    else [o.strip() for o in _RAW_ORIGINS.split(",") if o.strip()]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------

def get_tools(request: Request):
    return request.app.state.tools


def get_checkpointer(request: Request):
    return request.app.state.checkpointer


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Liveness probe — always returns 200 once uvicorn is up."""
    return {"status": "ok", "ready": _startup_complete, "error": _startup_error}


@app.get("/health/ready")
def health_ready():
    """Readiness probe — returns 503 until background tool loading finishes."""
    if not _startup_complete:
        return JSONResponse(status_code=503, content={"status": "starting", "ready": False})
    if _startup_error:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "ready": False, "error": _startup_error},
        )
    return {"status": "ok", "ready": True}


@app.post("/v1/chat", response_model=ChatResponse)
async def chat(
    body: ChatRequest,
    tools: list = Depends(get_tools),
    checkpointer=Depends(get_checkpointer),
    settings: Settings = Depends(get_settings),
):
    thread_id = body.session_id or str(uuid.uuid4())
    try:
        callbacks = _langfuse_callbacks(settings)
        answer, model, tier, citations = await run_chicago_agent(
            settings,
            tools,
            checkpointer,
            body.question,
            thread_id,
            callbacks=callbacks,
        )
    except Exception as e:
        logger.exception("POST /v1/chat failed thread_id=%s", thread_id)
        raise HTTPException(status_code=500, detail=_public_error_detail(e)) from e
    return ChatResponse(
        answer=answer,
        citations=citations,
        model=model,
        router_tier=tier,
        thread_id=thread_id,
    )


@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    """
    WebSocket streaming endpoint.

    Protocol:
      1. Client sends ONE JSON frame:
           {"question": "…", "session_id": "…"}   (session_id optional)
      2. Server streams JSON frames for each event type:
           {"type": "thinking", "text": "…"}
           {"type": "tool_call",   "tool": "…", "label": "…", "input_summary": "…"}
           {"type": "tool_result", "tool": "…", "label": "…", "result_summary": "…"}
           {"type": "token",  "text": "…"}    ← append these to build the answer
           {"type": "done",   "answer": "…", "citations": […], "thread_id": "…", …}
           {"type": "error",  "detail": "…"}
      3. Connection is closed by the server after "done" or "error".

    Origin restriction: set ALLOWED_ORIGINS env var (comma-separated) to lock down
    which frontend hosts may connect.
    """
    # ------ origin check ------
    origin = websocket.headers.get("origin", "")
    if ALLOWED_ORIGINS != ["*"] and origin not in ALLOWED_ORIGINS:
        await websocket.close(code=4403, reason="Origin not allowed")
        return

    await websocket.accept()

    settings = get_settings()
    tools = websocket.app.state.tools
    checkpointer = websocket.app.state.checkpointer

    try:
        raw = await websocket.receive_text()
        import json as _json
        payload = _json.loads(raw)
        question = payload.get("question", "").strip()
        if not question:
            await websocket.send_text(
                WsErrorEvent(detail="'question' field is required and must not be empty.").model_dump_json()
            )
            await websocket.close()
            return
        session_id = payload.get("session_id") or None
        thread_id = session_id or str(uuid.uuid4())

        callbacks = _langfuse_callbacks(settings)

        async for event in stream_chicago_agent(
            settings,
            tools,
            checkpointer,
            question,
            thread_id,
            callbacks=callbacks,
        ):
            await websocket.send_text(event.model_dump_json())
            if isinstance(event, (WsDoneEvent, WsErrorEvent)):
                break

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected cleanly")
    except Exception as exc:
        logger.exception("WebSocket /ws/chat error")
        try:
            await websocket.send_text(
                WsErrorEvent(detail=_public_error_detail(exc)).model_dump_json()
            )
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
