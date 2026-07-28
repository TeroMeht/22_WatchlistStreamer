"""
reversal_long breakout strategy -- orchestrator.

Runs on every incoming 5-sec bar (realtime path).

Fire semantics:
    * Setup filter: at least one of the last N 2-min candles has
      ``Relatr >= CAPITULATION_THRESHOLD`` (recent panic selling).
    * Reference: rolling max High of the last two 2-min candles
      (see ``reference.get_reference_from_last_two_candles``).
    * Breakout: current 5-sec bar closes strictly ABOVE the reference.
    * One-shot per session per symbol -- once fired, latched until the
      streamer is restarted.

MVP fire = signal alarm only (no entry order). To enable orders, pass a
``stop_level=<float>`` to ``fire_signal`` in the fire block below;
``src.strategies.actions.fire_signal`` will then also insert the entry
order.
"""

from __future__ import annotations

import logging
from typing import NamedTuple
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.core.config import settings
from src.helpers.handle_candles import stream_data_to_candle_row

from src.strategies.actions import fire_signal
from .filters.filters import evaluate_filters
from .reference import get_reference_from_last_two_candles
from .state import has_fired, mark_fired

logger = logging.getLogger(__name__)

# Signal name used in the alarm row + Telegram message when this strategy fires.
REVERSAL_LONG_SIGNAL_NAME: str = "reversal_long breakout"


# =============================================================================
# Breakout signal detection
# =============================================================================


class BreakoutEvent(NamedTuple):
    is_breakout: bool
    reason: str


def detect_breakout(livestream_last: float, breakout_level: float) -> BreakoutEvent:
    """True when the incoming 5-sec price is strictly above the breakout level."""
    if livestream_last > breakout_level:
        return BreakoutEvent(
            True,
            f"BREAKOUT (incoming livestream price {livestream_last:.2f} > level {breakout_level:.2f})",
        )
    return BreakoutEvent(False, "no breakout detected")


# =============================================================================
# Phase 3 + Phase 4: detect breakout, log, fire
# =============================================================================


async def _handle_possible_breakout(
    incoming_data_stream: RealTimeBar,
    symbol: str,
    breakout_level,
    bar_time_local,
    filters_summary: str,
) -> None:
    live_price = float(incoming_data_stream.close)
    event = detect_breakout(live_price, breakout_level.ref_close)

    logger.info(
        "reversal_long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
        "price=%.2f low=%.2f | %s | %s",
        symbol,
        bar_time_local.time(), live_price,
        breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
        filters_summary,
        event.reason,
    )

    if not event.is_breakout:
        return

    # --- Fire (alarm only for MVP -- pass stop_level to also insert order) ---
    candle = stream_data_to_candle_row(symbol, incoming_data_stream, bar_time_local)
    await fire_signal(candle, REVERSAL_LONG_SIGNAL_NAME)

    logger.info(
        "reversal_long breakout FIRED: %s -- Incoming livestream %s price=%.2f > "
        "Breakout level %s price=%.2f (stop_anchor=%.2f)",
        symbol,
        bar_time_local.time(), live_price,
        breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
    )

    # Latch the strategy for this symbol -- no further fires until restart.
    mark_fired(symbol)


# =============================================================================
# Strategy orchestrator
# =============================================================================


async def reversal_long_strategy(bar: RealTimeBar, symbol: str) -> None:
    """
    5-sec-bar-driven reversal_long breakout entry.

    Same call signature as the ORB realtime strategies so it plugs into
    ``REALTIME_ENTRY_STRATEGIES`` in the registry with no wrapper.
    """
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )

    # --- Session one-shot latch -------------------------------------------
    if has_fired(symbol):
        logger.debug(
            "reversal_long: %s already fired this session -- latched until restart "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 1: reference selection ---------------------------------------
    breakout_level = await get_reference_from_last_two_candles(symbol)
    if breakout_level is None:
        logger.debug(
            "reversal_long: %s -- reference not available yet "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 2: setup filters --------------------------------------------
    filters_passed, filters_summary = await evaluate_filters(
        symbol, float(bar.close), breakout_level, bar_time_local,
    )

    if filters_passed:
        await _handle_possible_breakout(
            bar, symbol, breakout_level, bar_time_local, filters_summary,
        )
