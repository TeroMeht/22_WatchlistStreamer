"""
Thin async wrapper around the Polygon aggregates REST API.

Only the two endpoints the replay-mode warmup needs are wrapped:

  * ``fetch_daily_bars(client, symbol, start_day, end_day)``     -> daily OHLCV
                                                                    (feeds ATR14).
  * ``fetch_intraday_bars(client, symbol, start_day, end_day,    -> N-minute
     minutes=2)``                                                   aggregates
                                                                    (feeds RVOL /
                                                                    avg-volume tables).

Deliberately standalone -- do NOT import from ``32_smsystem``. The two
projects are separate deployables; a cross-repo import would couple their
release cadence.

Pattern mirrors ``backend/datapipe/sources/datasource.py`` in the
smsystem project: one HTTP call per fetch, ``limit`` sized so a single
page holds the whole window, and a **loud failure** (RuntimeError) if
Polygon ever returns ``next_url``. Silent truncation would mean a
warmup DataFrame is short a few days, and the RVOL/ATR values downstream
would drift without any log line to explain why.

Every function takes a ``PolygonClient`` (session + api_key + base_url)
so the caller owns the aiohttp lifecycle -- typically one session per
call to ``_fetch_history_data`` in ``datastreamer.py``, opened via
``async with``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import aiohttp


logger = logging.getLogger(__name__)


@dataclass
class PolygonClient:
    """
    Small state container -- session + api key + base url.

    Kept as a plain dataclass rather than a class with methods so the
    fetch functions stay free functions (parallel to smsystem's shape)
    and are trivially unit-testable with a fake session.
    """

    session: aiohttp.ClientSession
    api_key: str
    base_url: str


def _iso(d: date) -> str:
    """Polygon accepts YYYY-MM-DD or unix-ms; we always send YYYY-MM-DD."""
    return d.isoformat()


async def _get_bars(
    client: PolygonClient,
    url: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    One GET against a Polygon aggregates endpoint. Returns the flat
    ``results[]`` list (each entry has short keys ``t/o/h/l/c/v/vw/n``).

    Fails loud (RuntimeError) if the response carries ``next_url`` --
    callers size ``limit`` well above the worst-case row count for their
    window, so pagination should never fire. If it does, bump ``limit``
    or shrink the window rather than adding pagination here (the warmup
    is a small fixed-size fetch, not a general-purpose historian).

    Logs elapsed time, byte count, and rate-limit headers so slow calls
    or 429s are visible in the streamer log.
    """
    page_params = {**params, "apiKey": client.api_key}

    t0 = time.monotonic()
    async with client.session.get(url, params=page_params) as resp:
        body_bytes = await resp.read()
        elapsed = time.monotonic() - t0

        rl_hdrs = {
            k: v for k, v in resp.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"
        }

        if resp.status >= 400:
            logger.warning(
                "[polygon] <-- %d %s (%.2fs, %d bytes) headers=%s body=%s",
                resp.status, url, elapsed, len(body_bytes),
                rl_hdrs, body_bytes[:400].decode("utf-8", errors="replace"),
            )
            resp.raise_for_status()

        logger.debug(
            "[polygon] <-- %d %s (%.2fs, %d bytes) headers=%s",
            resp.status, url, elapsed, len(body_bytes), rl_hdrs,
        )
        if rl_hdrs:
            logger.info("[polygon] rate-limit headers on %s: %s", url, rl_hdrs)

        data = json.loads(body_bytes)

    if data.get("next_url"):
        raise RuntimeError(
            f"[polygon] {url} returned next_url -- request exceeded 'limit'. "
            f"Bump the limit or shrink the window."
        )

    # Polygon returns {"status": "OK", "results": [...]} on success and
    # {"status": "NOT_AUTHORIZED" | "ERROR", ...} on plan/config issues.
    # ``resp.raise_for_status`` already covers HTTP-level errors; this
    # covers the case where Polygon returns 200 but with a soft error
    # (e.g. plan doesn't include this endpoint).
    status = data.get("status")
    if status not in ("OK", "DELAYED"):
        raise RuntimeError(
            f"[polygon] {url} returned status={status!r} "
            f"message={data.get('message') or data.get('error')!r}"
        )

    return data.get("results") or []


async def fetch_daily_bars(
    client: PolygonClient,
    symbol: str,
    start_day: date,
    end_day: date,
) -> list[dict[str, Any]]:
    """
    Daily OHLCV in ``[start_day, end_day]`` inclusive (ET session dates).

    Feeds the ATR14 warmup path. Caller should ask for enough calendar
    days to cover 14+ trading days after weekends/holidays (~21 calendar
    days is a safe minimum).
    """
    url = (
        f"{client.base_url}/v2/aggs/ticker/{symbol}"
        f"/range/1/day/{_iso(start_day)}/{_iso(end_day)}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": 5000}
    return await _get_bars(client, url, params)


async def fetch_intraday_bars(
    client: PolygonClient,
    symbol: str,
    start_day: date,
    end_day: date,
    minutes: int = 2,
) -> list[dict[str, Any]]:
    """
    N-minute aggregate bars in ``[start_day, end_day]`` (ET session dates).

    Feeds the RVOL / avg-volume warmup. Polygon aggregates the bars
    server-side at ``minutes``-minute cadence, so the caller receives
    already-aggregated bars matching the streamer's 2-min candle grid.

    ``limit=50000`` handles the worst case comfortably: a 5-calendar-day
    window at 2-min cadence with pre/post-market is ~1,950 bars.
    """
    url = (
        f"{client.base_url}/v2/aggs/ticker/{symbol}"
        f"/range/{minutes}/minute/{_iso(start_day)}/{_iso(end_day)}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    return await _get_bars(client, url, params)
