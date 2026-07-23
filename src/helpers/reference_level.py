"""
Per-symbol reference level for the ORB-long realtime strategy.

The reference is the 2-min candle labeled 16:32 in ``settings.TIMEZONE``
(the second RTH candle for a Helsinki-local streamer). Once that row is
present in ``{symbol}_livestream`` we cache its Close and Low, and every
subsequent 5-sec bar can compare against ``ref_close`` (trigger) and
``ref_low`` (stop level).

The cache is lazy: the first 5-sec bar for a symbol that arrives at or
after 16:34 will trigger a single DB fetch; if the row does not yet
exist we return ``None`` and try again on the next bar. The cache is
auto-invalidated when the local date changes, so restarting the streamer
mid-session reuses today's cached value but a fresh trading day forces a
re-read.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, time
from typing import Optional

from src.dependencies import get_db_pool
from src.core.config import settings

logger = logging.getLogger(__name__)


# The candle label we treat as the reference. Kept as a module constant so
# it's obvious where to change it if the definition of the ORB anchor moves.
REFERENCE_TIME: time = time(16, 32)


@dataclass(frozen=True)
class ReferenceLevel:
    """Immutable snapshot of the 16:32 candle for one symbol on one date."""
    symbol: str
    ref_date: date
    ref_time: time      # the reference candle's Time (16:32 in production)
    ref_close: float
    ref_low: float


# In-memory cache keyed by uppercase symbol.
_cache: dict[str, ReferenceLevel] = {}
# One lock per symbol prevents concurrent 5-sec bars from firing duplicate
# DB reads while the first fetch is still in flight.
_locks: dict[str, asyncio.Lock] = {}


def _lock_for(symbol: str) -> asyncio.Lock:
    key = symbol.upper()
    lock = _locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _locks[key] = lock
    return lock


async def _fetch_reference_row(symbol: str, day: date) -> Optional[ReferenceLevel]:
    """
    Read the 16:32 row for ``symbol`` on ``day`` from the livestream table.
    Returns ``None`` if the row has not been written yet (i.e. we're still
    inside the 16:32-16:34 candle) or the query errored.
    """
    table_name = f"{symbol.lower()}_livestream"
    query = f"""
        SELECT "Time", "Close", "Low"
        FROM "{table_name}"
        WHERE "Date" = $1 AND "Time" = $2
        LIMIT 1;
    """
    pool = get_db_pool()
    conn = await pool.acquire()
    try:
        row = await conn.fetchrow(query, day, REFERENCE_TIME)
    except Exception:
        logger.exception(
            "reference_level: failed to read %s row for %s on %s",
            REFERENCE_TIME, symbol, day,
        )
        return None
    finally:
        await pool.release(conn)

    if row is None:
        # Row not yet written -- caller will retry on the next 5-sec bar.
        return None

    return ReferenceLevel(
        symbol=symbol.upper(),
        ref_date=day,
        ref_time=row["Time"],
        ref_close=float(row["Close"]),
        ref_low=float(row["Low"]),
    )


async def get_reference_level(symbol: str, today: date) -> Optional[ReferenceLevel]:
    """
    Return the cached 16:32 reference for ``symbol`` on ``today``, fetching
    from the DB on first use or after a date rollover. ``None`` means the
    reference isn't available yet (candle not closed / row not written).
    """
    key = symbol.upper()
    cached = _cache.get(key)
    if cached is not None and cached.ref_date == today:
        return cached

    async with _lock_for(key):
        # Re-check under the lock so concurrent 5-sec bars don't double-fetch.
        cached = _cache.get(key)
        if cached is not None and cached.ref_date == today:
            return cached

        fetched = await _fetch_reference_row(symbol, today)
        if fetched is not None:
            _cache[key] = fetched
            logger.info(
                "reference_level cached: %s -- 2m candle %s %s close=%.2f low=%.2f",
                fetched.symbol, fetched.ref_date, fetched.ref_time,
                fetched.ref_close, fetched.ref_low,
            )
        return fetched


def clear_cache() -> None:
    """Test / manual-reset hook. Not called in normal operation."""
    _cache.clear()
    _locks.clear()
