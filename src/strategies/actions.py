"""
Shared fire pipeline for every strategy.

Any change to what happens on a fire (extra step, new side-effect, a
metric) belongs HERE -- every strategy funnels through this one call.
"""

from __future__ import annotations

import logging
from datetime import datetime
from types import ModuleType
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from src.alarms.alarm_generator import generate_signal_alarm
from src.core.config import settings
from src.helpers.handle_candles import CandleRow
from src.orders.order_generator import generate_entry_order

logger = logging.getLogger(__name__)


async def fire_signal(
    *,
    candle: CandleRow,
    signal_name: str,
    stop_level: float,
    viz: ModuleType,
    ref_close: Optional[float] = None,
    live_price: Optional[float] = None,
    mark_fired: Optional[Callable[[str], None]] = None,
) -> None:

    symbol = candle.symbol

    bar_time_local = datetime.combine(candle.date, candle.time).replace(
        tzinfo=ZoneInfo(settings.TIMEZONE),
    )

    if live_price is None:
        live_price = float(candle.close)
    if ref_close is None:
        ref_close = float(candle.close)

    stop_level = float(stop_level)

    await generate_entry_order(candle=candle, stop_level=stop_level)
    await generate_signal_alarm(candle=candle, signal_name=signal_name)

    viz.record_fire(
        symbol, bar_time_local, live_price,
        stop_level=stop_level,
        ref_close=ref_close,
    )

    if mark_fired is not None:
        mark_fired(symbol)

    logger.info(
        "fire_signal: %s %s signal=%s close=%.2f ref=%.2f stop=%.2f",
        symbol, bar_time_local, signal_name, live_price, ref_close, stop_level,
    )
