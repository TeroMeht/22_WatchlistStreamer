"""
Actions taken when the ORB long strategy decides to fire.

No filtering, no gating -- if you called ``fire_signal`` you already
decided to fire. Composition happens in the strategy body.
"""

from __future__ import annotations



from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order
from src.helpers.handle_candles import CandleRow



async def fire_signal(candle: CandleRow, stop_level: float) -> None:
    """Push the alarm and insert the entry order."""
    await generate_signal_alarm(candle=candle, signal_name="ORB long breakout")
    await generate_entry_order(candle=candle, stop_level=stop_level)
