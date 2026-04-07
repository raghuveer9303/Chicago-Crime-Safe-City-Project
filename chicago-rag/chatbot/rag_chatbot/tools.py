import json
import threading
import time
from typing import Any

import httpx
from langchain_core.tools import StructuredTool

# Global: at most one LangSearch web-search request per second (across all callers).
_LANGSEARCH_MIN_INTERVAL_SEC = 1.0
_langsearch_lock = threading.Lock()
_langsearch_next_allowed = 0.0

# Retry transient failures (5xx/429/network) a couple of times.
# Note: we still enforce the global 1 call/sec throttle for each attempt.
_LANGSEARCH_MAX_RETRIES = 2
_LANGSEARCH_RETRY_BACKOFF_BASE_SEC = 1.0


def _acquire_langsearch_rate_limit() -> None:
    global _langsearch_next_allowed
    with _langsearch_lock:
        now = time.monotonic()
        wait = _langsearch_next_allowed - now
        if wait > 0:
            time.sleep(wait)
        _langsearch_next_allowed = time.monotonic() + _LANGSEARCH_MIN_INTERVAL_SEC


def make_langsearch_tool(api_key: str) -> StructuredTool:
    url = "https://api.langsearch.com/v1/web-search"

    def _search(query: str) -> str:
        """Search the web for the given query string (keywords or question)."""
        body: dict[str, Any] = {
            "query": query,
            "count": 5,
            "summary": True,
            "freshness": "noLimit",
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        last_err: Exception | None = None
        for attempt in range(_LANGSEARCH_MAX_RETRIES + 1):
            _acquire_langsearch_rate_limit()
            try:
                with httpx.Client(timeout=60.0) as client:
                    r = client.post(url, json=body, headers=headers)
                    r.raise_for_status()
                    payload = r.json()
                data = payload.get("data") or {}
                pages = (data.get("webPages") or {}).get("value") or []
                out = []
                for p in pages[:5]:
                    out.append(
                        {
                            "name": p.get("name"),
                            "url": p.get("url"),
                            "snippet": p.get("snippet"),
                            "summary": p.get("summary"),
                            "datePublished": p.get("datePublished"),
                        }
                    )
                return json.dumps(out, indent=2)[:48000]
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                last_err = exc
                if attempt >= _LANGSEARCH_MAX_RETRIES:
                    raise

                # Retry after 429 / transient upstream server errors.
                if status in (429, 500, 502, 503, 504):
                    retry_after = exc.response.headers.get("retry-after")
                    if retry_after:
                        try:
                            time.sleep(float(retry_after))
                        except ValueError:
                            pass
                    else:
                        time.sleep(_LANGSEARCH_RETRY_BACKOFF_BASE_SEC * (2**attempt))
                    continue
                raise
            except httpx.RequestError as exc:
                last_err = exc
                if attempt >= _LANGSEARCH_MAX_RETRIES:
                    raise
                time.sleep(_LANGSEARCH_RETRY_BACKOFF_BASE_SEC * (2**attempt))
                continue

        # Should be unreachable; loop always returns or raises.
        raise last_err or RuntimeError("LangSearch call failed")

    return StructuredTool.from_function(
        name="langsearch_web_search",
        description=(
            "Search the public web via LangSearch for news, definitions, or context not in the graph. "
            "Returns titles, URLs, and snippets/summaries."
        ),
        func=_search,
    )
