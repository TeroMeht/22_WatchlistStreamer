"""
Reference-candle builders for the ORB long strategy.

Two builders live here; the strategy imports and calls ONE of them
directly:

    * ``get_reference_from_last_two_candles(symbol)`` -- derives the
      reference from the LAST TWO 2-min candles in
      ``{symbol}_livestream``. Handy for testing at any time of day.
    * ``get_reference_from_opening_range(symbol, day, or_start, or_end)``
      -- derives the reference from the OPENING RANGE: every 2-min
      candle on ``day`` with ``or_start <= Time <= or_end``. Defaults
      to 16:30-16:32 (two 2-min candles). Returns ``None`` until the
      closing OR candle has been written.

Both return a ``SimpleNamespace`` exposing ``.symbol``, ``.ref_time``,
``.ref_open``, ``.ref_close``, ``.ref_low``, ``.ref_field``. Callers
should not depend on the concrete type -- attribute access is the
contract. Neither builder caches: results are always fresh from the DB.
"""

from __future__ import annotations

import logging
from datetime import date, time
from types import SimpleNamespace
from typing import Optional

from src.database.db_functions import get_last_rows, get_session_rows

logger = logging.getLogger(__name__)


async def get_reference_from_last_two_candles(symbol: str) -> Optional[SimpleNamespace]:
    """
    Reference from the last two 2-min candles: highest High and lowest
    Low across both. Returns ``None`` if fewer than 2 candles exist.
    """
    df_last = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=2)

    if df_last.empty or len(df_last) < 2:
        return None

    idx_max_high = df_last["High"].idxmax()
    source_row = df_last.loc[idx_max_high]
    highest = float(source_row["High"])
    lowest = float(df_last["Low"].min())
    newest = df_last.iloc[-1]

    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=source_row["Time"],       # timestamp of the max-high candle
        ref_open=float(newest["Open"]),    # trigger-candle open, for green-candle filter
        ref_close=highest,                  # legacy attribute name; here it's "level to watch"
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

    or_df = df[df["Time"] <= or_end]
    if or_df.empty:
        return None

    # Wait for the range to be complete: the closing OR candle must be present.
    if or_df["Time"].max() < or_end:
        return None

    idx_max_high = or_df["High"].idxmax()
    source_row = or_df.loc[idx_max_high]
    first_row = or_df.iloc[0]

    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=source_row["Time"],           # timestamp of the max-high candle
        ref_open=float(first_row["Open"]),     # OR-window opening price, for green-candle filter
        ref_close=float(source_row["High"]),   # "level to watch" -- the OR high
        ref_low=float(or_df["Low"].min()),
        ref_field="high",
    )
