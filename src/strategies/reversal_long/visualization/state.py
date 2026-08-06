"""
reversal_long -- per-symbol overlay state for the dashboard.

Owns only the pieces that are actually reversal-specific:
    * ``_recent_relatrs`` / ``_recent_max_relatr`` -- rolling max Relatr
      across the last ``RECENT_RELATR_WINDOW`` finalized 2-min candles,
      used by the dashboard's "recent capitulation" setup check.
    * ``snapshot()`` -- decorates the shared overlay/fires/candle-timeline
      snapshot with ``recent_max_relatr`` per symbol and exposes
      ``capitulation_threshold`` / ``recent_relatr_window`` at the top
      level so the dashboard doesn't need to hardcode them.

Everything else (reference/fire storage, snapshot core, candle timeline)
lives in ``src.strategies.overlay_state`` and ``src.strategies.candle_timeline``
so this module and the ORB counterpart don't diverge.
"""

from __future__ import annotations

from typing import List, Optional

from src.core.config import settings
from src.strategies import candle_timeline, overlay_state


STRATEGY_KEY: str = "reversal"

# How many recent 2-min Relatr values feed ``recent_max_relatr``. Must
# match ``filters.filters.CAPITULATION_LOOKBACK_CANDLES`` so the
# dashboard shows the same window the strategy actually gates on.
RECENT_RELATR_WINDOW: int = 3


# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = candle_timeline.record_5s_tick
record_finalized_2min_candle = candle_timeline.record_finalized_2min_candle
seed_from_history            = candle_timeline.seed_from_history


# =============================================================================
# reversal-specific per-symbol metric
# =============================================================================


_recent_relatrs:     dict[str, List[float]]     = {}
_recent_max_relatr:  dict[str, Optional[float]] = {}


def record_relatr(symbol: str, relatr: Optional[float]) -> None:
    """
    Push the latest 2-min Relatr. Maintains a rolling buffer of the last
    ``RECENT_RELATR_WINDOW`` values so ``recent_max_relatr`` reflects the
    same lookback the strategy filter gates on.
    """
    if relatr is None:
        return
    key = symbol.upper()
    buf = _recent_relatrs.setdefault(key, [])
    buf.append(float(relatr))
    if len(buf) > RECENT_RELATR_WINDOW:
        del buf[:-RECENT_RELATR_WINDOW]
    _recent_max_relatr[key] = max(buf)
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
    Reversal dashboard snapshot: shared overlay/fires/candle-timeline
    shape plus ``recent_max_relatr`` per symbol, and the top-level
    ``capitulation_threshold`` / ``recent_relatr_window`` the dashboard
    reads for its capitulation-check label.
    """
    result = overlay_state.snapshot(
        STRATEGY_KEY,
        extra_symbol_fields=lambda sym: {
            "recent_max_relatr": _recent_max_relatr.get(sym.upper()),
        },
    )
    result["capitulation_threshold"] = float(settings.CAPITULATION_THRESHOLD)
    result["recent_relatr_window"] = RECENT_RELATR_WINDOW
    return result


def reset() -> None:
    overlay_state.reset(STRATEGY_KEY)
    _recent_relatrs.clear()
    _recent_max_relatr.clear()
