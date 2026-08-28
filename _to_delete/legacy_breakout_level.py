"""
Shared breakout-level (reference) builders for entry strategies.

Two builders live here; each strategy imports and calls whichever one
fits its setup:

    * ``get_reference_from_last_two_candles(symbol)`` -- derives the
      breakout level from the LAST TWO FINALIZED 2-min candles held in
      ``candle_timeline`` (the domain-level in-memory store also read by
      the dashboard viz layer).
      Rolling: reflects whatever the two most recent finalized candles
      are at call time; new finalizations shift the window on the next
      call. Synchronous, no DB hit -- safe to call from every 5-sec tick.

    * ``get_reference_from_opening_range(symbol, day, or_start, or_end)``
      -- derives the breakout level from the OPENING RANGE: every 2-min
      candle on ``day`` with ``or_start <= Time <= or_end``. Defaults
      to 16:30-16:32 (two 2-min candles). Returns ``None`` until the
      closing OR candle has been written; callers just retry on the
      next bar. Used by ORB long.

Both return a ``SimpleNamespace`` exposing the same attribute contract
so downstream code (hooks, filters, dashboard) doesn't care which
builder produced the level:

    .symbol       -- uppercase symbol
    .ref_time     -- Time of the candle that owns the level
    .ref_open     -- Open of the "trigger" candle (for green-candle filters)
    .ref_close    -- LEVEL to watch (the price a live bar must break)
    .ref_low      -- STOP anchor
    .ref_field    -- which OHLC field became the level; "high" for both

Attribute access is the contract; the concrete type may change.
Neither builder caches: results are always fresh from the DB.
"""

from __future__ import annotations

from datetime import date, time
from types import SimpleNamespace
from typing import Optional

from src.database.db_functions import get_session_rows
from src.strategies import candle_timeline


def get_reference_from_last_two_candles(symbol: str) -> Optional[SimpleNamespace]:
    """
    Reference from the last two FINALIZED 2-min candles: highest High
    and lowest Low across both. Returns ``None`` if fewer than 2 finalized
    candles exist yet.

    Reads straight from ``candle_timeline`` (in-memory store populated
    by ``seed_from_history`` at startup and ``record_finalized_2min_candle``
    on every finalize). No DB call -- this runs on every 5-sec tick, so
    the previous ``get_last_rows`` round-trip was pure duplication.
    Synchronous; callers no longer ``await``.
    """
    candles = candle_timeline.get_last_finalized_candles(symbol, 2)
    if len(candles) < 2:
        return None

    max_high_candle = max(candles, key=lambda c: c["high"])
    highest = float(max_high_candle["high"])
    lowest = float(min(c["low"] for c in candles))
    newest = candles[-1]

    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=max_high_candle["dt"].time(),  # datetime.time of the max-high candle
        ref_open=float(newest["open"]),          # trigger-candle open, for green-candle filter
        ref_close=highest,                        # "level to watch" -- legacy attr name
        ref_low=lowest,
        ref_field="high",
    )


async def get_reference_from_opening_range(
    symbol: str,
    day: date,
    or_start: time = time(16, 30),
    or_end: time = time(16, 32),
) -> Optional[SimpleNamespace]:
    """
    Opening-range reference: build the breakout level from every 2-min
    candle on ``day`` with ``or_start <= Time <= or_end``. Defaults are
    16:30-16:32 (two 2-min candles); override to widen or shift the
    window.

    Fields on the returned object:
      * ``ref_close`` = max High across the OR window (the breakout level)
      * ``ref_low``   = min Low  across the OR window (the stop anchor)
      * ``ref_open``  = Open of the FIRST candle in the window
      * ``ref_time``  = Time of the candle that owns ``ref_close``
      * ``ref_field`` = ``"high"``

    Returns ``None`` until the ``or_end`` candle has been written to
    ``{symbol}_livestream`` (i.e. the range is complete). Callers should
    just retry on the next bar.
    """
    df = await get_session_rows(
        table_name=f"{symbol.lower()}_livestream",
        day=day,
        since_time=or_start,
    )
    if df.empty:
        return None

    or_df = df[df["time"] <= or_end]
    if or_df.empty:
        return None

    # Wait for the range to be complete: the closing OR candle must be present.
    if or_df["time"].max() < or_end:
        return None

    idx_max_high = or_df["high"].idxmax()
    source_row = or_df.loc[idx_max_high]
    first_row = or_df.iloc[0]

    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=source_row["time"],           # timestamp of the max-high candle
        ref_open=float(first_row["open"]),     # OR-window opening price, for green-candle filter
        ref_close=float(source_row["high"]),   # "level to watch" -- the OR high
        ref_low=float(or_df["low"].min()),
        ref_field="high",
    )
