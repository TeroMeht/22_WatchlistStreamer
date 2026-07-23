"""
Realtime (5-sec) entry strategies.

These run on every incoming 5-sec bar rather than only on finalized 2-min
candles, so they can trigger the moment price crosses a level. Kept in
their own module so the existing 2-min entry strategies in
``src/entry_strategies.py`` stay untouched.

Currently implemented:
    * orb_breakout_long -- fires when a 5-sec bar closes above the 16:32
      candle's Close. Stop level is that candle's Low minus $0.02. One-shot
      per symbol per day.
"""

from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order
from src.database.db_functions import get_last_rows
from src.helpers.handle_candles import CandleRow
from src.helpers.reference_level import get_reference_level
from src.core.config import settings

logger = logging.getLogger(__name__)


# ORB long stop is anchored $0.02 below the reference candle's Low. Kept as
# a module constant so it's easy to promote to a settings field later.
ORB_STOP_OFFSET: float = 0.02

# Minimum Rvol on the most recent 2-min candle for an ORB long to be allowed
# to fire. Same rationale as above -- module constant now, promotable later.
ORB_MIN_RVOL: float = 3.0

# One-shot latch: {SYMBOL_UPPER: date_fired}. Prevents re-firing the same
# breakout every subsequent 5-sec bar during the same session.
_orb_fired: dict[str, date] = {}


def _bar_to_candle_row(symbol: str, bar: RealTimeBar) -> CandleRow:
    """
    Build a minimal ``CandleRow`` from a 5-sec ``RealTimeBar`` so we can
    reuse ``generate_signal_alarm`` and ``generate_entry_order`` -- both
    expect a CandleRow. Indicator fields (vwap/ema9/relatR/rvol/avg_volume)
    aren't meaningful for a 5-sec bar so we zero them; only symbol/date/
    time/close matter for the downstream alarm + order writers.
    """
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    close_px = float(bar.close)
    return CandleRow(
        symbol=symbol,
        date=bar_time_local.date(),
        time=bar_time_local.time(),
        open=close_px,
        high=close_px,
        low=close_px,
        close=close_px,
        volume=float(bar.volume),
        vwap=0.0,
        ema9=0.0,
        avg_volume=0.0,
        rvol=0.0,
        relatR=0.0,
    )


async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:
    """
    Fire a long alarm + entry order when the 5-sec ``bar`` closes above
    the 16:32 reference candle's Close. Stop = ref_low - $0.02.

    Skips silently when:
        * the reference level isn't available yet (16:32 candle not written)
        * we've already fired for this symbol today.
    """
    key = symbol.upper()
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    today = bar_time_local.date()

    # One-shot per day per symbol.
    if _orb_fired.get(key) == today:
        return

    ref = await get_reference_level(symbol, today)
    if ref is None:
        # Reference candle isn't in the DB yet; try again on the next bar.
        return

    if float(bar.close) <= ref.ref_close:
        return

    # --- Rvol filter --------------------------------------------------------
    # Price is above the reference; now check that the most recent completed
    # 2-min candle has Rvol >= ORB_MIN_RVOL. We only hit the DB *after* the
    # price condition passes so quiet symbols cost nothing. We do NOT latch on
    # a filter miss -- a later 5-sec bar can still fire once volume picks up.
    df_last = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream", num_rows=1,
    )
    if df_last.empty or "Rvol" not in df_last.columns:
        logger.debug(
            "ORB long: %s price above ref but no recent candle / Rvol column; "
            "skipping without latch", symbol,
        )
        return
    latest_rvol = float(df_last.iloc[-1]["Rvol"])
    if latest_rvol < ORB_MIN_RVOL:
        logger.debug(
            "ORB long filter miss: %s bar.close=%.2f > ref_close=%.2f but "
            "latest Rvol=%.2f < %.2f -- no latch, will retry on next bar",
            symbol, float(bar.close), ref.ref_close, latest_rvol, ORB_MIN_RVOL,
        )
        return

    # Latch immediately so any 5-sec bars racing in behind this one skip out.
    _orb_fired[key] = today

    stop_level = round(ref.ref_low - ORB_STOP_OFFSET, 2)
    logger.info(
        "ORB long breakout: %s at %s bar.close=%.2f > ref_close=%.2f "
        "(ref_low=%.2f, stop=%.2f, Rvol=%.2f)",
        symbol, bar_time_local.time(), float(bar.close),
        ref.ref_close, ref.ref_low, stop_level, latest_rvol,
    )

    candle = _bar_to_candle_row(symbol, bar)
    await generate_signal_alarm(candle=candle, signal_name="ORB long breakout")
    await generate_entry_order(candle=candle, stop_level=stop_level)
