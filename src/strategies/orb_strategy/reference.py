"""
Reference-candle selection for the ORB long strategy.

Two modes:
    * Production (``ORB_TEST_MODE_USE_LAST_CANDLE = False``): reference
      is the 16:32 candle's CLOSE (the breakout level) with its LOW as
      the stop anchor and its OPEN carried on the ref so filters can
      inspect the candle body (e.g. green-candle filter). Cached
      in-process per symbol per date; the first 5-sec bar arriving at
      or after 16:34 triggers one DB read. If the row isn't written yet
      we return ``None`` and try again on the next bar. Auto-invalidated
      on date rollover.
    * Test (``ORB_TEST_MODE_USE_LAST_CANDLE = True``): reference is
      derived from the LAST TWO 2-min candles in ``{symbol}_livestream``:
      the level to watch is ``max(High)`` of the two, the stop anchor
      is ``min(Low)`` of the two, and ``ref_open`` is the Open of the
      newer of the two (the trigger candle). Not cached.

Both modes return an object exposing ``.symbol``, ``.ref_time``,
``.ref_open``, ``.ref_close`` and ``.ref_low`` (a ``ReferenceLevel``
dataclass in production, a ``SimpleNamespace`` in test mode). Callers
should not depend on the concrete type -- attribute access is the
contract.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, time
from types import SimpleNamespace
from typing import Optional, Tuple

from src.database.db_functions import get_last_rows, get_livestream_row_at_time

from .config import ORB_TEST_MODE_USE_LAST_CANDLE

logger = logging.getLogger(__name__)


# The candle label we treat as the production reference. Kept as a module
# constant so it's obvious where to change it if the ORB anchor moves.
REFERENCE_TIME: time = time(16, 32)


# =============================================================================
# Production reference: 16:32 candle, cached per symbol per date
# =============================================================================


@dataclass(frozen=True)
class ReferenceLevel:
    """
    Immutable snapshot of the 16:32 opening-range candle for one symbol
    on one date.
      * ``ref_close`` -- breakout level (bar must close above this to fire)
      * ``ref_low``   -- stop anchor; stop = ref_low - ORB_STOP_OFFSET
      * ``ref_open``  -- candle body reference, used by the green-candle
                          filter (passes when ``ref_close > ref_open``)
    """
    symbol: str
    ref_date: date
    ref_time: time      # the reference candle's Time (16:32 in production)
    ref_open: float     # 16:32 candle Open
    ref_close: float    # 16:32 candle Close (breakout level)
    ref_low: float      # 16:32 candle Low (stop anchor)


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
    Read the 16:32 row for ``symbol`` on ``day`` from the livestream
    table and map it into a ``ReferenceLevel``. Uses the candle's CLOSE
    as the breakout level (``ref_close``), its LOW as the stop anchor
    (``ref_low``), and carries the OPEN so filters can inspect the
    candle body. Returns ``None`` if the row has not been written yet
    or the DB read errored.
    """
    row = await get_livestream_row_at_time(symbol, day, REFERENCE_TIME)
    if row is None:
        return None
    return ReferenceLevel(
        symbol=symbol.upper(),
        ref_date=day,
        ref_time=row["Time"],
        ref_open=float(row["Open"]),
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
                "reference_level cached: %s -- 2m candle %s %s open=%.2f close=%.2f low=%.2f",
                fetched.symbol, fetched.ref_date, fetched.ref_time,
                fetched.ref_open, fetched.ref_close, fetched.ref_low,
            )
        return fetched


def clear_cache() -> None:
    """Test / manual-reset hook. Not called in normal operation."""
    _cache.clear()
    _locks.clear()


# =============================================================================
# Test-mode reference: high/low of the last two candles, always fresh
# =============================================================================


async def _get_reference_from_last_two_candles(symbol: str) -> Optional[SimpleNamespace]:
    """
    Test-mode reference: highest High and lowest Low across the last two
    2-min candles in ``{symbol}_livestream``. Returns ``None`` if fewer
    than 2 candles exist.
    """
    df_last = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream", num_rows=2,
    )
    if df_last.empty or len(df_last) < 2:
        return None
    # get_last_rows orders ascending by Date, Time -- iloc[-1] is newest.
    highest = float(df_last["High"].max())
    lowest = float(df_last["Low"].min())
    newest = df_last.iloc[-1]
    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=newest["Time"],
        ref_open=float(newest["Open"]),   # trigger-candle open, for green-candle filter
        ref_close=highest,                 # legacy attribute name; here it's "level to watch"
        ref_low=lowest,
    )


# =============================================================================
# Top-level dispatcher used by the strategy
# =============================================================================


async def select_reference(symbol: str, today: date) -> Tuple[Optional[object], str]:
    """
    Return ``(reference, label)``. ``reference`` is ``None`` if the
    reference isn't available yet (16:32 candle not written in
    production, or fewer than 2 candles in test mode). ``label`` is a
    short string for logging.
    """
    if ORB_TEST_MODE_USE_LAST_CANDLE:
        return await _get_reference_from_last_two_candles(symbol), "LAST-2-CANDLES (test mode)"
    return await get_reference_level(symbol, today), "16:32"
