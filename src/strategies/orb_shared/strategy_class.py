from __future__ import annotations

import logging
from datetime import time
from types import ModuleType, SimpleNamespace
from zoneinfo import ZoneInfo

from data_sources._bar import IncomingBar

from src.core.config import settings
from src.database.db_functions import get_session_rows

from src.strategies.actions import fire_signal
from src.strategies.hooks import make_hooks

from .breakout_level import get_reference_from_opening_range
from .detection import detect_breakout

logger = logging.getLogger(__name__)


class ORBStrategy:
    """
    Callable orchestrator for one direction of the ORB strategy.

    Instantiate once per direction; invoke ``await instance.run(bar, symbol)``
    from the streamer's realtime dispatch loop (see ``strategies.py``).
    """

    def __init__(
        self,
        *,
        direction: str,
        signal_name: str,
        viz: ModuleType,
        filters: ModuleType,
        or_start: time = time(16, 30),
        or_end: time = time(16, 32),
    ) -> None:
        direction = direction.lower()
        if direction not in ("long", "short"):
            raise ValueError(f"ORBStrategy direction must be 'long' or 'short', got {direction!r}")

        self.direction = direction
        self.signal_name = signal_name
        self.viz = viz
        self.filters = filters
        self.or_start = or_start
        self.or_end = or_end

        self.hooks = make_hooks(viz)

        # Per-instance one-shot fire latch. Restart wipes it.
        self._fired: dict[str, bool] = {}

        # Human-readable prefix used in every log line so the two
        # directions are easy to tell apart in tail output.
        self._log_prefix = f"ORB {direction}"

    # ------------------------------------------------------------------
    # Fire latch (instance-owned; no shared state module needed)
    # ------------------------------------------------------------------

    def mark_fired(self, symbol: str) -> None:
        self._fired[symbol.upper()] = True

    def has_fired(self, symbol: str) -> bool:
        return self._fired.get(symbol.upper(), False)

    # ------------------------------------------------------------------
    # Direction plumbing
    # ------------------------------------------------------------------

    def _direction_reference(self, ref) -> SimpleNamespace:
        """
        The opening-range builder returns:
            ref.ref_close = max High across OR window (breakout level for long)
            ref.ref_low   = min Low  across OR window (stop anchor for long)

        For long we pass the object through unchanged. For short we swap:
        the breakdown level is the OR low, and the stop anchor is the OR
        high. Downstream hooks + fire pipeline just read ``ref_close``
        and ``ref_low``; keeping the same shape means the viz layer stays
        direction-agnostic.
        """
        if self.direction == "long":
            return ref
        return SimpleNamespace(
            symbol=ref.symbol,
            ref_time=ref.ref_time,
            ref_open=ref.ref_open,
            ref_close=ref.ref_low,   # breakdown level
            ref_low=ref.ref_close,   # stop anchor (max OR high)
            ref_field="low",
        )

    async def _compute_stop_level(self, symbol: str, today, bar, fallback_stop: float) -> float:
        """
        Long:  stop = min(session lows, current bar low)  - offset
        Short: stop = max(session highs, current bar high) + offset
        """
        df_session = await get_session_rows(
            table_name=f"{symbol.lower()}_livestream",
            day=today,
            since_time=settings.SESSION_START,
        )
        if df_session.empty:
            logger.warning(
                "%s: %s -- no session bars in livestream to anchor stop; "
                "falling back to opening-range extreme",
                self._log_prefix, symbol,
            )
            if self.direction == "long":
                return round(fallback_stop - settings.ORB_STOP_OFFSET, 2)
            return round(fallback_stop + settings.ORB_STOP_OFFSET, 2)

        if self.direction == "long":
            ref_price = min(float(df_session["low"].min()), float(bar.low))
            return round(ref_price - settings.ORB_STOP_OFFSET, 2)
        # short
        ref_price = max(float(df_session["high"].max()), float(bar.high))
        return round(ref_price + settings.ORB_STOP_OFFSET, 2)

    # ------------------------------------------------------------------
    # Main orchestrator (called once per 5-sec bar)
    # ------------------------------------------------------------------

    async def run(self, bar: IncomingBar, symbol: str) -> None:

        bar_time_local = bar.date.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))
        today = bar_time_local.date()

        # Chart animates on every tick regardless of what branch we take below.
        self.hooks.on_bar(symbol, bar_time_local, bar)

        # --- Session one-shot latch ---------------------------------------
        if self.has_fired(symbol):
            logger.debug(
                "%s: %s already fired this session -- latched until restart "
                "(LIVE 5s bar %s close=%.2f)",
                self._log_prefix, symbol, bar_time_local.time(), float(bar.close),
            )
            return

        # --- Phase 1: setup filters --------------------------------------
        # Delegate to the strategy's OWN filter module -- direction-specific
        # gates live there; this class stays filter-agnostic.
        filters_passed, filter_results = await self.filters.evaluate_filters(
            symbol, float(bar.close),
        )
        self.viz.record_filter_results(symbol, filter_results)
        filters_summary = self.filters.format_summary(filter_results)

        # --- Phase 2: reference selection --------------------------------
        raw_ref = await get_reference_from_opening_range(
            symbol, today, or_start=self.or_start, or_end=self.or_end,
        )
        if raw_ref is None:
            logger.info(
                "%s: %s -- opening-range reference not available yet "
                "(LIVE 5s bar %s close=%.2f) | %s",
                self._log_prefix, symbol, bar_time_local.time(),
                float(bar.close), filters_summary,
            )
            return

        breakout_level = self._direction_reference(raw_ref)
        self.hooks.on_reference(symbol, breakout_level)

        # --- Phase 3: filter gate ----------------------------------------
        if not filters_passed:
            logger.info(
                "%s: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
                "price=%.2f low=%.2f | %s",
                self._log_prefix, symbol,
                bar_time_local.time(), float(bar.close),
                breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
                filters_summary,
            )
            return

        # --- Phase 4: breakout / breakdown detection ---------------------
        event = detect_breakout(float(bar.close), breakout_level.ref_close, self.direction)
        logger.info(
            "%s: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
            "price=%.2f low=%.2f | %s | %s",
            self._log_prefix, symbol,
            bar_time_local.time(), float(bar.close),
            breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
            filters_summary,
            event.reason,
        )
        if not event.is_breakout:
            return

        # --- Phase 5: stop level -----------------------------------------
        stop_level = await self._compute_stop_level(
            symbol, today, bar,
            fallback_stop=float(breakout_level.ref_low),
        )

        # --- Phase 6: fire alarm + generate order ------------------------
        await fire_signal(
            bar, symbol, breakout_level, bar_time_local, stop_level,
            signal_name=self.signal_name,
            hooks=self.hooks,
            mark_fired=self.mark_fired,
        )
