"""
reversal_short -- per-symbol overlay state for the dashboard.

Mirrors ``reversal_long/visualization/state.py`` with one difference:
``STRATEGY_KEY = "reversal_short"`` so overlay/fires slices do not
collide with the long side in the shared store. The snapshot exposes
the per-symbol ``filters`` list; nothing else short-specific rides on
it (yesterday levels aren't relevant to this rolling-2-bar setup).
"""

from __future__ import annotations

from typing import List

from src.strategies import candle_timeline, overlay_state


STRATEGY_KEY: str = "reversal_short"


# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = candle_timeline.record_5s_tick
record_finalized_2min_candle = candle_timeline.record_finalized_2min_candle
seed_from_history            = candle_timeline.seed_from_history


# =============================================================================
# reversal_short-specific per-symbol filter results
# =============================================================================


_latest_filters: dict[str, List[dict]] = {}


def record_filter_results(symbol: str, results) -> None:
    _latest_filters[symbol.upper()] = [
        {
            "id":     r.id,
            "label":  r.label,
            "passed": bool(r.passed),
            "detail": r.detail,
        }
        for r in results
    ]
    overlay_state.touch(STRATEGY_KEY, symbol)


# =============================================================================
# Hook contract -- thin wrappers over the shared store, bound to this
# strategy's key. ``make_hooks(viz)`` calls these.
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
    reversal_short dashboard snapshot: shared overlay/fires/candle-timeline
    shape plus the per-symbol ``filters`` list.
    """
    return overlay_state.snapshot(
        STRATEGY_KEY,
        extra_symbol_fields=lambda sym: {
            "filters": _latest_filters.get(sym.upper(), []),
        },
    )


def reset() -> None:
    overlay_state.reset(STRATEGY_KEY)
    _latest_filters.clear()
