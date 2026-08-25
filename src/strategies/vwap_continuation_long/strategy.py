"""
vwap_continuation_long -- 2-min candle-driven orchestrator.

Runs from ``run_strategies(candle)`` on every finalized 2-min
``CandleRow``. Does NOT run on 5-sec bars: the trigger (EMA9 crossover
UP) is a candle-scale event, and every gate the filters check reads
finalized rows from ``{symbol}_livestream``, so per-tick evaluation
would only produce noise.

Setup story:
    Price extended above VWAP (prior euforia), cooled off all the way
    back to VWAP, and now prints an EMA9 crossover UP -- the second
    leg begins. See ``filters/filters.py`` for the four filters that
    enforce that journey candle-by-candle; every one must pass on this
    candle for the strategy to fire.

Fire semantics:
    * All four filters must pass on the current finalized 2-min candle.
      If any fails, no order, no alarm.
    * Stop level: shared ``detect_stoplevel`` over the last
      ``STOP_LOOKBACK_CANDLES`` livestream rows (long direction).
    * Fire:
        - Telegram + alarm-row via ``generate_signal_alarm``
        - Entry order via ``generate_entry_order`` (parked for user
          acceptance in the trade backend)
        - Dashboard fire marker via ``viz.record_fire``
        - Latch via ``mark_fired`` -- one-shot per session per symbol
    * Filter evaluation still runs (and publishes to the dashboard)
      after the latch has fired so the check rows keep updating.

The dashboard card renders the same filter check rows and fire markers
the ORB / reversal_long cards do -- see ``visualization/state.py`` for
the wiring. Because this strategy is candle-driven, only finalized
2-min candles animate the chart for symbols where this is the ONLY
armed strategy -- no per-tick growing candle.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from src.alarms.alarm_generator import generate_signal_alarm
from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.helpers.handle_candles import CandleRow
from src.orders.order_generator import detect_stoplevel, generate_entry_order

from .filters.filters import evaluate_filters, format_summary
from .state import has_fired, mark_fired
from .visualization import state as viz

logger = logging.getLogger(__name__)


# Signal name used in the alarm row + Telegram message when this strategy fires.
VWAP_CONTINUATION_LONG_SIGNAL_NAME: str = "VWAP continuation long"

# How many recent 2-min candles feed the stop-level calculation. Same
# livestream source the filters read from; long direction.
STOP_LOOKBACK_CANDLES: int = 5


# =============================================================================
# Strategy orchestrator
# =============================================================================


async def vwap_continuation_long_strategy(candle: CandleRow) -> None:

    symbol = candle.symbol

    # --- Phase 1: setup filters --------------------------------------------
    # Evaluate on every finalized candle regardless of the latch -- the
    # dashboard rows should keep animating so we can see whether the
    # setup decayed right after firing. The fire latch below is what
    # stops duplicate entries.
    filters_passed, filter_results = await evaluate_filters(symbol)
    viz.record_filter_results(symbol, filter_results)
    filters_summary = format_summary(filter_results)

    # --- Phase 2: one-shot latch -------------------------------------------
    if has_fired(symbol):
        logger.debug(
            "vwap_continuation_long: %s already fired this session -- latched "
            "until restart (2m candle %s close=%.2f) | %s",
            symbol, candle.time, float(candle.close), filters_summary,
        )
        return

    # --- Phase 3: filter gate ----------------------------------------------
    if not filters_passed:
        logger.info(
            "vwap_continuation_long: %s at %s close=%.2f | %s",
            symbol, candle.time, float(candle.close), filters_summary,
        )
        return

    # --- Phase 4: detect stop level ----------------------------------------
    df_last = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream", num_rows=STOP_LOOKBACK_CANDLES,
    )
    stop_level = detect_stoplevel(df_last, direction="long")

    logger.info(
        "vwap_continuation_long: %s at %s close=%.2f stop=%.2f | %s",
        symbol, candle.time, float(candle.close), stop_level, filters_summary,
    )

    # --- Phase 5: fire alarm + entry order + viz marker + latch -----------
    await generate_signal_alarm(candle=candle, signal_name=VWAP_CONTINUATION_LONG_SIGNAL_NAME)
    await generate_entry_order(candle=candle, stop_level=stop_level)

    # Record the fire on the dashboard. Build a tz-aware datetime so
    # the marker lands on the correct 2-min interval. ``ref_close`` is
    # cosmetic for the marker -- pass the candle close so the record
    # is self-consistent.
    bar_dt = datetime.combine(candle.date, candle.time).replace(
        tzinfo=ZoneInfo(settings.TIMEZONE),
    )
    viz.record_fire(
        symbol, bar_dt, float(candle.close),
        stop_level=float(stop_level),
        ref_close=float(candle.close),
    )
    mark_fired(symbol)
