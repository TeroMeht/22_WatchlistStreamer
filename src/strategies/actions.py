"""
Strategy-wide fire pipeline.

One shared entry point every fire path calls after the strategy has
decided to fire (Phase 3 confirmed a breakout AND a stop level has been
computed). Runs the full ceremony end-to-end:

    1. build a CandleRow from the 5-sec bar
    2. send the signal alarm        (Telegram + alarms row)
    3. insert the entry order       (orders row)
    4. hooks.on_fire(...)           -- viz fire marker + stop line
    5. mark_fired(symbol)           -- one-shot latch until restart

Callers pass their strategy identity (``signal_name``, ``hooks``,
``mark_fired``) on every call -- there is no factory / binding step.
``stop_level`` is required -- every fire inserts an entry order.

No detection, no gating -- if you called ``fire_signal``, the strategy
already decided to fire.
"""

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
