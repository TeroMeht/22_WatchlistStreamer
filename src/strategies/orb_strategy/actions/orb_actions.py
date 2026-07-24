"""
Actions taken when the ORB long strategy decides to fire.

Two responsibilities:
    * Build a ``CandleRow`` from a 5-sec ``RealTimeBar`` because the
      downstream alarm / order writers expect a CandleRow.
    * Send the signal alarm (Telegram + DB row) AND insert the entry
      order into the ``orders`` table.

No filtering, no gating -- if you called ``fire_signal`` you already
decided to fire. Composition happens in the strategy body.
"""

from __future__ import annotations

from datetime import datetime

from ib_async import RealTimeBar

from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order
from src.helpers.handle_candles import CandleRow


ORB_SIGNAL_NAME: str = "ORB long breakout"


def bar_to_candle_row(symbol: str, bar: RealTimeBar, bar_time_local: datetime) -> CandleRow:
    """
    Minimal CandleRow synthesized from a 5-sec bar. Indicator fields
    (vwap/ema9/relatR/rvol/avg_volume) aren't meaningful for a 5-sec
    bar so we zero them; only symbol/date/time/close matter for the
    downstream alarm + order writers.
    """
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


async def fire_signal(candle: CandleRow, stop_level: float) -> None:
    """Push the alarm and insert the entry order."""
    await generate_signal_alarm(candle=candle, signal_name=ORB_SIGNAL_NAME)
    await generate_entry_order(candle=candle, stop_level=stop_level)
