"""
reversal_long breakout strategy -- orchestrator.

Runs on every incoming 5-sec bar (realtime path).

Fire semantics:
    * Setup filter: at least one of the last N 2-min candles has
      ``Relatr >= CAPITULATION_THRESHOLD`` (recent panic selling).
    * Reference: rolling max High of the last two FINALIZED 2-min
      candles, read from the in-memory ``candle_timeline``
      (see ``breakout_level.get_reference_from_last_two_candles``).
    * Breakout: current 5-sec bar closes strictly ABOVE the reference.
    * Stop:   ``min(Low across last STOP_LOOKBACK_CANDLES) -
      ORB_STOP_OFFSET`` computed via the shared ``detect_stoplevel``.
    * Fire:   ``src.strategies.actions.fire_signal`` runs the full
      pipeline -- viz hook + alarm + entry order + viz fire hook + latch.
    * One-shot per session per symbol -- once fired, latched until the
      streamer is restarted.

Phase order is deliberate: filter evaluation runs BEFORE the reference
availability check so the dashboard populates its filter rows from the
very first 5-sec tick after warmup -- users don't want to stare at
"waiting for first tick" until two finalized 2-min candles exist.
"""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.orders.order_generator import detect_stoplevel

from src.strategies.actions import fire_signal
from src.strategies.breakout_level import get_reference_from_last_two_candles
from src.strategies.detection import detect_breakout
from src.strategies.hooks import make_hooks
from .filters.filters import evaluate_filters, format_summary
from .state import has_fired, mark_fired
from .visualization import state as viz

logger = logging.getLogger(__name__)

# Signal name used in the alarm row + Telegram message when this strategy fires.
REVERSAL_LONG_SIGNAL_NAME: str = "reversal_long breakout"

# How many recent 2-min candles feed the stop-level calculation. Stop is
# ``min(Low across last STOP_LOOKBACK_CANDLES) - ORB_STOP_OFFSET`` -- same
# livestream source the capitulation filter reads from, wider window so
# the stop sits well below the recent trading range.
STOP_LOOKBACK_CANDLES: int = 5

# Bind the shared hook factory to this strategy's viz state module.
hooks = make_hooks(viz)


# =============================================================================
# Strategy orchestrator
# =============================================================================


async def reversal_long_strategy(bar: RealTimeBar, symbol: str) -> None:

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))

    # Chart animates on every tick regardless of what branch we take below.
    hooks.on_bar(symbol, bar_time_local, bar)

    # --- Session one-shot latch -------------------------------------------
    # Once the alarm has fired, we stop trailing the rolling 2-bar high --
    # the reference line freezes at its fire-time value (same behavior as
    # ORB long). Subsequent bars still animate the candle series but skip
    # every hook that would move the level.
    if has_fired(symbol):
        logger.debug(
            "reversal_long: %s already fired this session -- latched until restart "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 1: setup filters --------------------------------------------
    # Run and publish BEFORE the reference check so the dashboard card
    # populates from tick #1, not from tick #1 that arrives after two
    # 2-min candles have finalized.
    filters_passed, filter_results = await evaluate_filters(symbol)
    viz.record_filter_results(symbol, filter_results)
    filters_summary = format_summary(filter_results)

    # --- Phase 2: reference selection ---------------------------------------
    breakout_level = get_reference_from_last_two_candles(symbol)
    if breakout_level is None:
        logger.debug(
            "reversal_long: %s -- reference not available yet "
            "(LIVE 5s bar %s close=%.2f) | %s",
            symbol, bar_time_local.time(), float(bar.close), filters_summary,
        )
        return

    hooks.on_reference(symbol, breakout_level)

    # --- Phase 3: filter gate ----------------------------------------------
    if not filters_passed:
        logger.info(
            "reversal_long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
            "price=%.2f low=%.2f | %s",
            symbol,
            bar_time_local.time(), float(bar.close),
            breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
            filters_summary,
        )
        return

    # --- Phase 4: breakout detection ---------------------------------------
    event = detect_breakout(float(bar.close), breakout_level.ref_close)
    
    logger.info(
        "reversal_long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
        "price=%.2f low=%.2f | %s | %s",
        symbol,
        bar_time_local.time(), float(bar.close),
        breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
        filters_summary,
        event.reason,
    )
    if not event.is_breakout:
        return

    # --- Phase 5: detect stop level ---------------------------------------------
    df_last = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=STOP_LOOKBACK_CANDLES)
    stop_level = detect_stoplevel(df_last, direction="long")


    # --- Phase 6: fire alarm and generate order ---------------------------------------------
    await fire_signal(
        bar, symbol, breakout_level, bar_time_local, stop_level,
        signal_name=REVERSAL_LONG_SIGNAL_NAME,
        hooks=hooks,
        mark_fired=mark_fired,
    )
