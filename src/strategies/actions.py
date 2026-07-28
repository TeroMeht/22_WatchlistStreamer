"""
Strategy-wide action layer.

Single shared entry point that every fire path (ORB, reversal_long, and
any future breakout-style strategy) calls once the strategy has decided
to fire. Composition happens in each strategy body -- if you called
``fire_signal`` you already decided to fire; this module does not filter
or gate.

Two responsibilities, cleanly split by the ``stop_level`` argument:

* ``fire_signal(candle, signal_name)`` -- alarm only. Pushes the
  signal to Telegram + writes the alarms row. Used by MVP strategies
  that are notification-only until they've been validated live.

* ``fire_signal(candle, signal_name, stop_level=X)`` -- alarm + entry
  order. Also inserts a row into the ``orders`` table via
  ``generate_entry_order``.
"""

from __future__ import annotations

from typing import Optional

from src.alarms.alarm_generator import generate_signal_alarm
from src.helpers.handle_candles import CandleRow
from src.orders.order_generator import generate_entry_order


async def fire_signal(
    candle: CandleRow,
    signal_name: str,
    stop_level: Optional[float] = None,
) -> None:
    """
    Push the signal alarm; also insert the entry order when
    ``stop_level`` is supplied.

    Pass ``stop_level=None`` (or omit) for alarm-only strategies.
    """
    await generate_signal_alarm(candle=candle, signal_name=signal_name)
    if stop_level is not None:
        await generate_entry_order(candle=candle, stop_level=stop_level)
