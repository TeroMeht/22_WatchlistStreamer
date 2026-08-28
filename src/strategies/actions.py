from __future__ import annotations

from types import SimpleNamespace
from typing import Callable

from ib_async import RealTimeBar

from src.alarms.alarm_generator import generate_signal_alarm
from src.helpers.handle_candles import stream_data_to_candle_row
from src.orders.order_generator import generate_entry_order


async def fire_signal(
    incoming_data_stream: RealTimeBar,
    symbol: str,
    breakout_level,
    bar_time_local,
    stop_level: float,
    *,
    signal_name: str,
    hooks: SimpleNamespace,
    mark_fired: Callable[[str], None],
) -> None:
    """
    Run the full fire pipeline for a confirmed breakout. See module
    docstring for the step-by-step sequence.
    """
    live_price = float(incoming_data_stream.close)

    candle = stream_data_to_candle_row(symbol, incoming_data_stream, bar_time_local)
    await generate_entry_order(candle=candle, stop_level=stop_level)
    await generate_signal_alarm(candle=candle, signal_name=signal_name)
    

    hooks.on_fire(
        symbol, bar_time_local, live_price,
        breakout_level.ref_close,
        stop_level=stop_level,
    )
    mark_fired(symbol)
