"""
Shared reversal strategy orchestrator -- CANDLE-DRIVEN.

Reversal is a 2-min candle strategy (same cadence as vwap_continuation_long),
not a 5-sec realtime one. The trigger is EMA9 crossover on the last 2
finalized candles + capitulation (long) or euforia (short) somewhere in
the last 8 candles. Both conditions are evaluated inside the strategy's
own filters module -- the class stays direction-agnostic.

The two live instances live in ``reversal_long/strategy.py`` and
``reversal_short/strategy.py`` and only differ in:

    * ``direction``     -- ``"long"`` or ``"short"``; drives the stop
                            calculation (via ``detect_stoplevel``) and
                            fire logging.
    * ``signal_name``   -- string that appears in the Telegram / alarm row.
    * ``viz``           -- the strategy's visualization state module
                            (published to on filter eval and on fire).
    * ``filters``       -- the strategy's own filters module. Each
                            direction owns its own filter set (long ->
                            EMA9 crossover UP + recent capitulation,
                            short -> EMA9 crossover DOWN + recent
                            euforia). Filters are NOT shared between
                            directions.

Fire semantics (per instance, per symbol):
    * Filters evaluate on every finalized 2-min candle and publish to
      the dashboard card so the check rows keep updating between fires.
    * On a full pass, stop is anchored via the shared ``detect_stoplevel``
      over the last ``stop_lookback_candles`` livestream rows in the
      trade direction.
    * Fire pipeline: Telegram + alarm-row via ``generate_signal_alarm``,
      entry order via ``generate_entry_order``, dashboard marker via
      ``viz.record_fire``.
    * NO one-shot latch -- reversal can fire repeatedly in a session.
      Every fire is treated as a new trade: a fresh stop is calculated
      from the current window, a new order is submitted, and the
      dashboard replaces the displayed stop / stop-line with the newest
      fire's values (``fires[-1]`` in the JS). Natural spam prevention
      comes from the EMA9-crossover filter -- the crossover pattern
      resets once price is durably on one side of the EMA, so back-to-
      back candles rarely both pass.
"""

from __future__ import annotations

import logging
from datetime import datetime
from types import ModuleType
from zoneinfo import ZoneInfo

from src.alarms.alarm_generator import generate_signal_alarm
from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.helpers.handle_candles import CandleRow
from src.orders.order_generator import detect_stoplevel, generate_entry_order

logger = logging.getLogger(__name__)


# How many recent 2-min candles feed the stop-level calculation. Same
# livestream source the filters read from.
STOP_LOOKBACK_CANDLES: int = 8


class ReversalStrategy:
    """
    Callable orchestrator for one direction of the reversal strategy.

    Instantiate once per direction; invoke ``await instance.run(candle)``
    from the 2-min candle dispatch loop.
    """

    def __init__(
        self,
        *,
        direction: str,
        signal_name: str,
        viz: ModuleType,
        filters: ModuleType,
        stop_lookback_candles: int = STOP_LOOKBACK_CANDLES,
    ) -> None:
        direction = direction.lower()
        if direction not in ("long", "short"):
            raise ValueError(
                f"ReversalStrategy direction must be 'long' or 'short', got {direction!r}"
            )

        self.direction = direction
        self.signal_name = signal_name
        self.viz = viz
        # Direction-specific filter module -- each direction has its own
        # filter set; nothing is shared between long and short here.
        self.filters = filters
        self.stop_lookback_candles = stop_lookback_candles

        self._log_prefix = f"reversal_{direction}"

    # ------------------------------------------------------------------
    # Main orchestrator (called once per finalized 2-min candle)
    # ------------------------------------------------------------------

    async def run(self, candle: CandleRow) -> None:

        symbol = candle.symbol

        # --- Phase 1: setup filters --------------------------------------
        # Evaluate on every finalized candle so the dashboard rows keep
        # animating between fires.
        filters_passed, filter_results = await self.filters.evaluate_filters(symbol)
        self.viz.record_filter_results(symbol, filter_results)
        filters_summary = self.filters.format_summary(filter_results)

        # --- Phase 2: filter gate ----------------------------------------
        if not filters_passed:
            logger.info(
                "%s: %s at %s close=%.2f | %s",
                self._log_prefix, symbol, candle.time, float(candle.close), filters_summary,
            )
            return

        # --- Phase 3: stop level -----------------------------------------
        # Recomputed on EVERY fire -- each pass is a distinct trade with
        # its own stop derived from the current window.
        df_last = await get_last_rows(
            table_name=f"{symbol.lower()}_livestream",
            num_rows=self.stop_lookback_candles,
        )
        stop_level = detect_stoplevel(df_last, direction=self.direction)

        logger.info(
            "%s: %s at %s close=%.2f stop=%.2f | %s",
            self._log_prefix, symbol, candle.time, float(candle.close),
            stop_level, filters_summary,
        )

        # --- Phase 4: fire alarm + entry order + viz marker -------------
        # No fire latch: every full-pass candle is a new trade. The
        # dashboard already reads ``fires[-1]`` for the displayed stop
        # and stop-line, so appending here is what "updates the UI to
        # the newest trade".
        await generate_signal_alarm(candle=candle, signal_name=self.signal_name)
        await generate_entry_order(candle=candle, stop_level=stop_level)

        # Record the fire on the dashboard. Build a tz-aware datetime so
        # the marker lands on the correct 2-min interval. ``ref_close``
        # is cosmetic for the marker -- pass the candle close so the
        # record is self-consistent.
        bar_dt = datetime.combine(candle.date, candle.time).replace(
            tzinfo=ZoneInfo(settings.TIMEZONE),
        )
        self.viz.record_fire(
            symbol, bar_dt, float(candle.close),
            stop_level=float(stop_level),
            ref_close=float(candle.close),
        )
