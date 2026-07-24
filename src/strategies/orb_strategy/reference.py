"""
Reference-candle selection for the ORB long strategy.

Two modes:
    * Production (``ORB_TEST_MODE_USE_LAST_CANDLE = False``): reference is
      the 16:32 candle -- cached by ``src.helpers.reference_level``.
    * Test (``ORB_TEST_MODE_USE_LAST_CANDLE = True``): reference is
      derived from the LAST TWO 2-min candles in ``{symbol}_livestream``:
      the level to watch is ``max(High)`` of the two, and the stop
      anchor is ``min(Low)`` of the two. Not cached -- re-read on every
      5-sec tick so the reference rolls forward every 2 minutes.

Both modes return an object exposing ``.symbol``, ``.ref_time``,
``.ref_close`` and ``.ref_low`` (a ``ReferenceLevel`` dataclass in
production, a ``SimpleNamespace`` in test mode). Callers should not
depend on the concrete type -- attribute access is the contract.

Note: ``ref_close`` is a legacy name kept for interface parity with the
production reference. In test mode it holds the HIGH of the last two
candles, not a close price. Rename downstream if you promote test mode
to production.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Optional, Tuple

from src.database.db_functions import get_last_rows
from src.helpers.reference_level import get_reference_level

from .config import ORB_TEST_MODE_USE_LAST_CANDLE


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
    latest_time = df_last.iloc[-1]["Time"]
    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=latest_time,
        ref_close=highest,   # legacy name; here it's "level to watch"
        ref_low=lowest,
    )


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
