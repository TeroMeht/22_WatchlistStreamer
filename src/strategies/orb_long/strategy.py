"""
ORB long entry strategy -- orchestrator.

Runs on every incoming 5-sec bar. Wires together reference selection,
setup filters, breakout detection, and actions. Helpers live in sibling
modules; the breakout predicate lives inline just above the orchestrator
so the fire path reads top-to-bottom.

Fire semantics (current build):
    * Reference must be available: the OPENING RANGE (default 16:30-16:32)
      must be complete in the livestream table
      (see ``reference.get_reference_from_opening_range``).
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
from datetime import time
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.core.config import settings
from src.database.db_functions import get_last_rows

from src.strategies.actions import fire_signal
from src.strategies.breakout_level import (
    get_reference_from_last_two_candles,
    get_reference_from_opening_range,
)
from src.strategies.detection import detect_breakout
from src.strategies.hooks import make_hooks
from .filters.filters import evaluate_filters
from .state import has_fired, mark_fired
from .visualization import state as viz

logger = logging.getLogger(__name__)

# Signal name used in the alarm row + Telegram message when this strategy fires.
ORB_LONG_SIGNAL_NAME: str = "ORB long breakout"

# Bind the shared hook factory to this strategy's viz state module.
hooks = make_hooks(viz)



# =============================================================================
# Strategy orchestrator
# =============================================================================


async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))
    today = bar_time_local.date()

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
    # breakout_level = await get_reference_from_last_two_candles(symbol)

    breakout_level = await get_reference_from_opening_range(symbol, today, or_start=time(16, 30), or_end=time(16, 32))

    if breakout_level is None:
        logger.info(
            "ORB long: %s -- opening-range reference not available yet "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    hooks.on_reference(symbol, breakout_level)

    # --- Phase 2: setup filters --------------------------------------------

    filters_passed, filters_summary = await evaluate_filters(symbol, float(bar.close))

    if not filters_passed:
        logger.info(
            "ORB long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
            "price=%.2f low=%.2f | %s",
            symbol,
            bar_time_local.time(), float(bar.close),
            breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
            filters_summary,
        )
        return

    # --- Phase 3: breakout detection ---------------------------------------
    event = detect_breakout(float(bar.close), breakout_level.ref_close)
    logger.info(
        "ORB long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
        "price=%.2f low=%.2f | %s | %s",
        symbol,
        bar_time_local.time(), float(bar.close),
        breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
        filters_summary,
        event.reason,
    )
    if not event.is_breakout:
        return


    # --- Phase 4: detect stop level ---------------------------------------------
    # Stop sits below the low of the last COMPLETED candle in the livestream
    # (the candle immediately preceding the trigger), not below the
    # reference/opening-range low. Adapts to actual price action into the break.
    df_last = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=1)
    if df_last.empty:
        logger.warning(
            "ORB long: %s -- no prior candle in livestream to anchor stop; "
            "falling back to breakout_level.ref_low", symbol,
        )
        stop_reference = float(breakout_level.ref_low)
    else:
        stop_reference = float(df_last["Low"].iloc[-1])
    stop_level = round(stop_reference - settings.ORB_STOP_OFFSET, 2)

    # --- Phase 5: fire alarm and generate order ---------------------------------------------
    await fire_signal(
        bar, symbol, breakout_level, bar_time_local, stop_level,
        signal_name=ORB_LONG_SIGNAL_NAME,
        hooks=hooks,
        mark_fired=mark_fired,
    )


