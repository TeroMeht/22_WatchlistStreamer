"""
ORB long -- per-symbol overlay state for the dashboard.

Owns only the pieces that are actually ORB-specific:
    * ``_latest_rvol`` -- Rvol of the most recent finalized 2-min candle,
      compared to the Rvol threshold in the dashboard's setup checks.
    * ``snapshot()`` -- decorates the shared overlay/fires/candle-timeline
      snapshot with ``yesterday_high``, ``yesterday_close``, and
      ``latest_rvol``.

Everything else (reference/fire storage, snapshot core, candle timeline)
lives in ``src.strategies.overlay_state`` and ``src.strategies.candle_timeline``
so this module and the reversal counterpart don't diverge.
"""

from __future__ import annotations

from datetime import time
from typing import Optional

from src.core.config import settings
from src.strategies import candle_timeline, overlay_state
# Yesterday's daily OHLC is owned by the strategy state (used by the
# yesterday-level filters); the dashboard reads it from there so we
# don't keep two copies in sync.
from src.strategies.orb_long import state as strategy_state


STRATEGY_KEY: str = "orb"


# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = candle_timeline.record_5s_tick
record_finalized_2min_candle = candle_timeline.record_finalized_2min_candle
seed_from_history            = candle_timeline.seed_from_history


# =============================================================================
# ORB-specific per-symbol metric
# =============================================================================


_latest_rvol: dict[str, Optional[float]] = {}
# Rolling max of the finalized 2-min candles' highs -- mirrors what the
# ``check_premarket_high`` filter computes (max High across the livestream
# table) so the dashboard can display the same value and pass/fail state
# without re-reading the DB.
_premarket_high: dict[str, Optional[float]] = {}


def record_rvol(symbol: str, rvol: Optional[float]) -> None:
    """Update the latest 2-min candle's Rvol (called from finalize_candle)."""
    _latest_rvol[symbol.upper()] = None if rvol is None else float(rvol)
    overlay_state.touch(STRATEGY_KEY, symbol)


def record_premarket_high(
    symbol: str, high: Optional[float], candle_time: Optional[time] = None,
) -> None:
    """
    Fold a candle High into the rolling premarket max. Called from
    warmup (seeded once from the historical intraday frame, pre-filtered)
    and finalize_candle (each new finalized 2-min candle).

    ``candle_time`` is the 2-min candle's local time; when it's >=
    ``settings.SESSION_START`` the update is ignored so the value
    freezes at the true premarket high. ``None`` means "trust the
    caller" (used by warmup which has already filtered the frame).
    Passing ``high=None`` is also a no-op.
    """
    if high is None:
        return
    if candle_time is not None and candle_time >= settings.SESSION_START:
        return
    key = symbol.upper()
    current = _premarket_high.get(key)
    _premarket_high[key] = float(high) if current is None else max(current, float(high))
    overlay_state.touch(STRATEGY_KEY, symbol)


# =============================================================================
# Hook contract -- thin wrappers over the shared store, bound to this
# strategy's key. ``make_hooks(viz)`` calls these; strategies never touch
# the store directly.
# =============================================================================


def record_reference(symbol, ref_time, ref_close, ref_low, field=None) -> None:
    overlay_state.record_reference(
        STRATEGY_KEY, symbol, ref_time, ref_close, ref_low, field,
    )


def record_fire(symbol, bar_dt, close, stop_level, ref_close) -> None:
    overlay_state.record_fire(
        STRATEGY_KEY, symbol, bar_dt, close, stop_level, ref_close,
    )


# =============================================================================
# Reader
# =============================================================================


def snapshot() -> dict:
    """
    ORB dashboard snapshot: shared overlay/fires/candle-timeline shape
    plus ``yesterday_high``, ``yesterday_close``, and ``latest_rvol``
    per symbol.
    """
    return overlay_state.snapshot(
        STRATEGY_KEY,
        extra_symbol_fields=lambda sym: {
            "yesterday_high":  strategy_state.yesterday_high(sym),
            "yesterday_close": strategy_state.yesterday_close(sym),
            "latest_rvol":     _latest_rvol.get(sym.upper()),
            "premarket_high":  _premarket_high.get(sym.upper()),
        },
    )


def reset() -> None:
    overlay_state.reset(STRATEGY_KEY)
    _latest_rvol.clear()
    _premarket_high.clear()
