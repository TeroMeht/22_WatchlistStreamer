"""
Reference-candle builder for the reversal_long breakout strategy.

Derives the breakout level from the LAST TWO 2-min candles in
``{symbol}_livestream``:
    * level to watch (``ref_close``) = ``max(High)`` across the two
    * stop anchor    (``ref_low``)   = ``min(Low)`` across the two
    * ``ref_open`` = Open of the NEWER candle (the trigger), kept for
      downstream filters that inspect the candle body.

Returns a ``SimpleNamespace`` exposing ``.symbol``, ``.ref_time``,
``.ref_open``, ``.ref_close``, ``.ref_low``, ``.ref_field``. Callers
should not depend on the concrete type -- attribute access is the
contract. Not cached: always fresh from the DB.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

from src.database.db_functions import get_last_rows


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
