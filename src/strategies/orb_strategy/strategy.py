"""
ORB long entry strategy -- orchestrator.

Runs on every incoming 5-sec bar. Wires together reference selection,
setup filters, breakout detection, and actions. Helpers live in sibling
modules; the breakout predicate lives inline just above the orchestrator
so the fire path reads top-to-bottom.

Fire semantics (current build):
    * Reference must be available: at least two 2-min candles in the
      livestream table (see ``reference.get_reference_from_last_two_candles``).
    * ALL setup filters must pass on the current bar. If any filter
      fails, skip the breakout check entirely -- ``detect_breakout``
      is stateless so there's no crossing bookkeeping to preserve.
    * One-shot per session: once ORB fires for a symbol, the strategy
      is latched for that symbol until the streamer is restarted. The
      reference and stop lines stay in place on the chart at their
      fire-time values because subsequent bars still animate the
      candle series but skip every hook that would move the levels.
"""

from __future__ import annotations

import logging
from typing import NamedTuple
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar
from src.helpers.handle_candles import stream_data_to_candle_row
from src.core.config import settings

from .actions.orb_actions import fire_signal
from .filters.orb_filters import evaluate_filters
from .hooks import orb_hooks as hooks
from .reference import get_reference_from_last_two_candles
from .state import has_fired, mark_fired

logger = logging.getLogger(__name__)


# =============================================================================
# Breakout signal detection
# =============================================================================
# Stateless predicate: is the current bar's close above the reference?
# The session one-shot latch in ``state.py`` handles the "don't re-fire"
# concern, so this doesn't need to remember whether the previous bar was
# above ref.


class BreakoutEvent(NamedTuple):
    is_breakout: bool
    reason: str  # human-readable string for logging


def detect_breakout(livestream_last: float, breakout_level: float) -> BreakoutEvent:
    """True when ``bar_close`` is strictly above ``ref_close``."""
    if livestream_last > breakout_level:
        return BreakoutEvent(
            True,
            f"BREAKOUT (bar close {livestream_last:.2f} > ref close {breakout_level:.2f})",
        )
    return BreakoutEvent(
        False,
        f"no breakout yet (bar close {livestream_last:.2f} <= ref close {breakout_level:.2f})",
    )






async def _handle_possible_breakout(incoming_data_stream: RealTimeBar, symbol: str, breakout_level, bar_time_local) -> None:

    # --- Phase 3: breakout detection ----------------------------------------
    event = detect_breakout(float(incoming_data_stream.close), breakout_level.ref_close)

    logger.info(
        "ORB long: %s -- LiveStream last %s price=%.2f | Breakout level %s "
        "price=%.2f low=%.2f | %s",
        symbol,
        bar_time_local.time(), float(incoming_data_stream.close),
        breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
        event.reason,
    )

    if not event.is_breakout:
        return

    hooks.on_breakout(symbol)

    # --- Phase 4: fire ------------------------------------------------------
    stop_level = round(breakout_level.ref_low - settings.ORB_STOP_OFFSET, 2)
    logger.info(
        "ORB long breakout FIRED: %s -- LiveStream last %s price=%.2f > "
        "Breakout level %s price=%.2f (low=%.2f, stop=%.2f)",
        symbol,
        bar_time_local.time(), float(incoming_data_stream.close),
        breakout_level.ref_time, breakout_level.ref_close,
        breakout_level.ref_low, stop_level,
    )

    candle = stream_data_to_candle_row(symbol, incoming_data_stream, bar_time_local)
    await fire_signal(candle, stop_level)
    hooks.on_fire(symbol, bar_time_local, float(incoming_data_stream.close), stop_level, breakout_level.ref_close)
    # Latch the strategy for this symbol -- no further fires until restart.
    mark_fired(symbol)



# =============================================================================
# Strategy orchestrator
# =============================================================================
async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))

    # Chart animates on every tick regardless of what branch we take below.
    hooks.on_bar(symbol, bar_time_local, bar)

    # --- Session one-shot latch -------------------------------------------
    if has_fired(symbol):
        logger.debug(
            "ORB long: %s already fired this session -- latched until restart "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 1: reference selection ---------------------------------------
    breakout_level = await get_reference_from_last_two_candles(symbol)
    # or with a custom window:
    # breakout_level = await get_reference_from_opening_range(symbol, today, time(16, 30), time(16, 32))

    if breakout_level is None:
        logger.debug("ORB long: %s --reference not available yet "
                     "(LIVE 5s bar %s close=%.2f)",
                     symbol,  bar_time_local.time(), float(bar.close))
        return
    
    hooks.on_reference(symbol, breakout_level)

    # --- Phase 2: setup filters --------------------------------------------
    filters_passed = await evaluate_filters(symbol, float(bar.close), breakout_level, bar_time_local)

    if filters_passed:
        await _handle_possible_breakout(bar, symbol, breakout_level, bar_time_local)


