"""
ORB short -- per-symbol overlay state for the dashboard.

Mirrors ``orb_long/visualization/state.py`` with two differences:

    * ``STRATEGY_KEY = "orb_short"`` so overlay/fires slices do not
      collide with the long side in the shared store.
    * The snapshot decorates each symbol with ``yesterday_low`` and
      ``yesterday_close`` (short-side reference points) instead of
      ``yesterday_high``.

Yesterday's daily OHLC comes from ``orb_shared.yesterday`` so the two
directions share the same cache.
"""

from __future__ import annotations

from typing import List

from src.strategies import candle_timeline, overlay_state
from src.strategies.orb_shared import yesterday


STRATEGY_KEY: str = "orb_short"


# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = candle_timeline.record_5s_tick
record_finalized_2min_candle = candle_timeline.record_finalized_2min_candle
seed_from_history            = candle_timeline.seed_from_history


# =============================================================================
# ORB-short-specific per-symbol filter results
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
    ORB short dashboard snapshot: shared overlay/fires/candle-timeline
    shape plus ``yesterday_low``, ``yesterday_close``, and the per-symbol
    ``filters`` list.
    """
    return overlay_state.snapshot(
        STRATEGY_KEY,
        extra_symbol_fields=lambda sym: {
            "yesterday_low":   yesterday.yesterday_low(sym),
            "yesterday_close": yesterday.yesterday_close(sym),
            "filters":         _latest_filters.get(sym.upper(), []),
        },
    )


def reset() -> None:
    overlay_state.reset(STRATEGY_KEY)
    _latest_filters.clear()
