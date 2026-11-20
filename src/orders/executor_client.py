"""
HTTP client for the 26 backend's automatic executor (/api/executor/*).

Return values
-------------
dict   -> final answer: the executor answered (armed / rejected / failed),
          or the outcome is unknown (timeout, 5xx). Never retry these:
          every /arm places a new order, so a retry could duplicate one.
None   -> the request never reached the backend (connection refused,
          503 executor not running). Safe to retry.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from src.core.config import settings

logger = logging.getLogger(__name__)

# /arm waits for IB's acknowledgement (a few seconds) before replying.
_TIMEOUT = httpx.Timeout(10.0, connect=3.0)
_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=_TIMEOUT)
    return _client


async def _post(path: str, payload: Optional[dict] = None) -> Optional[dict]:
    url = f"{settings.EXECUTOR_BASE_URL.rstrip('/')}{path}"
    try:
        resp = await _get_client().post(url, json=payload)
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        logger.error("Executor POST %s: backend not reachable: %s", url, e)
        return None                                   # nothing was sent: safe to retry
    except httpx.HTTPError as e:
        logger.error("Executor POST %s: no answer (%s); order may or may not exist, "
                     "check /api/executor/status", url, e)
        return {"ok": False, "status": "unknown", "message": f"no answer: {e}"}

    if resp.status_code == 503:
        logger.error("Executor POST %s -> 503 (executor not running)", url)
        return None                                   # rejected before any order: safe to retry
    if resp.status_code >= 500:
        logger.error("Executor POST %s -> HTTP %s: %s", url, resp.status_code, resp.text[:300])
        return {"ok": False, "status": "unknown", "message": f"HTTP {resp.status_code}"}
    if resp.status_code >= 400:
        # 4xx = our request is wrong (e.g. 422 validation, 404 unknown id);
        # retrying the same body won't help.
        logger.error("Executor POST %s -> HTTP %s: %s", url, resp.status_code, resp.text[:300])
        return {"ok": False, "status": f"http_{resp.status_code}", "message": resp.text[:300]}
    return resp.json()


async def arm_setup(payload: dict) -> Optional[dict]:
    return await _post("/arm", payload)


async def cancel_setup(setup_id: str) -> Optional[dict]:
    return await _post(f"/cancel/{setup_id}")
