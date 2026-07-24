"""
ORB long entry strategy -- orchestrator.

Runs on every incoming 5-sec bar. Wires together reference selection,
setup filters, breakout detection, and actions. Helpers live in sibling
modules; the breakout predicate lives inline just above the orchestrator
so the fire path reads top-to-bottom.

Fire semantics (current build):
    * Reference must be available: the 16:32 candle in production, or
      at least two 2-min candles for the test-mode last-2-candles ref.
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

from src.core.config import settings

from .actions.orb_actions import bar_to_candle_row, fire_signal
from .config import ORB_STOP_OFFSET
from .filters.orb_filters import run_all_filters, format_filter_results
from .hooks import orb_hooks as hooks
from .reference import select_reference
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


def detect_breakout(bar_close: float, ref_close: float) -> BreakoutEvent:
    """True when ``bar_close`` is strictly above ``ref_close``."""
    if bar_close > ref_close:
        return BreakoutEvent(
            True,
            f"BREAKOUT (bar close {bar_close:.2f} > ref close {ref_close:.2f})",
        )
    return BreakoutEvent(
        False,
        f"no breakout yet (bar close {bar_close:.2f} <= ref close {ref_close:.2f})",
    )


# =============================================================================
# Strategy orchestrator
# =============================================================================


async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:
    """
    Fire a long alarm + entry order when the 5-sec ``bar`` closes above
    the reference level (16:32 candle's Close in production, or the
    highest High across the last two 2-min candles in test mode) with
    all setup filters passing.
    """
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    today = bar_time_local.date()

    # Chart animates on every tick regardless of what branch we take below.
    hooks.on_bar(symbol, bar_time_local, bar)

    # --- Session one-shot latch --------------------------------------------
    # After a fire, silently skip everything for the rest of the session.
    # The chart keeps animating candles (on_bar above) so the user can
    # watch price action, but no reference / filter / breakout hook fires
    # -- viz retains its last known ref_close, ref_low, and fire marker,
    # so the "level to watch" and "stop" lines stay frozen in place.
    if has_fired(symbol):
        logger.debug(
            "ORB long: %s already fired this session -- latched until restart "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 1: reference selection ---------------------------------------
    # Test mode uses the high/low of the last two 2-min candles;
    # production uses the cached 16:32 candle.
    ref, ref_label = await select_reference(symbol, today)
    if ref is None:
        logger.debug("ORB long: %s -- %s reference not available yet "
                     "(LIVE 5s bar %s close=%.2f)",
                     symbol, ref_label, bar_time_local.time(), float(bar.close))
        return
    hooks.on_reference(symbol, ref)

    # --- Phase 2: setup filters (ALL must pass) -----------------------------
    # If any filter fails we log the miss reasons and return -- the
    # breakout predicate is stateless so there's nothing to preserve.
    filter_results = await run_all_filters(symbol, float(bar.close), ref)
    
    if not all(r.passed for r in filter_results):
        logger.info(
            "ORB long: %s -- %s (LIVE 5s bar %s close=%.2f | REF close=%.2f)",
            symbol, format_filter_results(filter_results),
            bar_time_local.time(), float(bar.close), ref.ref_close,
        )
        return

    # --- Phase 3: breakout detection ----------------------------------------
    # Stateless: just compare bar close to ref close. Re-fire prevention
    # is handled entirely by the session latch checked at the top of this
    # function, so no crossing / continuation bookkeeping is needed here.
    event = detect_breakout(float(bar.close), ref.ref_close)

    logger.info(
        "ORB long check: %s -- LIVE 5s bar %s close=%.2f | REF 2m candle %s "
        "close=%.2f low=%.2f [%s] | %s -- %s",
        symbol,
        bar_time_local.time(), float(bar.close),
        ref.ref_time, ref.ref_close, ref.ref_low,
        ref_label,
        format_filter_results(filter_results),
        event.reason,
    )

    if event.is_breakout:
        hooks.on_breakout(symbol)

        # --- Phase 4: fire ------------------------------------------------------
        stop_level = round(ref.ref_low - ORB_STOP_OFFSET, 2)
        logger.info(
            "ORB long breakout FIRED: %s -- LIVE 5s bar %s close=%.2f > "
            "REF 2m candle %s close=%.2f (ref_low=%.2f, stop=%.2f) [%s]",
            symbol,
            bar_time_local.time(), float(bar.close),
            ref.ref_time, ref.ref_close,
            ref.ref_low, stop_level, ref_label,
        )

        candle = bar_to_candle_row(symbol, bar, bar_time_local)
        await fire_signal(candle, stop_level)
        hooks.on_fire(symbol, bar_time_local, float(bar.close), stop_level, ref.ref_close)
        # Latch the strategy for this symbol -- no further fires until restart.
        mark_fired(symbol)
