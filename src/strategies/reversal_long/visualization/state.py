"""
reversal_long -- per-symbol overlay state for the dashboard.

Owns only the pieces that are actually reversal-specific:
    * ``_latest_filters`` -- the last per-filter results from
      ``filters.evaluate_filters`` for this symbol. The strategy hands
      them in via ``record_filter_results`` on every 5-sec tick that
      reaches the filter phase; the dashboard renders each entry as a
      check row (label + pass/fail + detail) so the client stays
      agnostic about WHICH filters exist -- add/remove/rethreshold in
      ``filters.py`` and the UI follows on the next poll.
    * ``snapshot()`` -- decorates the shared overlay/fires/candle-timeline
      snapshot with the per-symbol ``filters`` list.

Everything else (reference/fire storage, snapshot core, candle timeline)
lives in ``src.strategies.overlay_state`` and ``src.strategies.candle_timeline``
so this module and the ORB counterpart don't diverge.
"""

from __future__ import annotations

from typing import List

from src.strategies import candle_timeline, overlay_state


STRATEGY_KEY: str = "reversal"


# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = candle_timeline.record_5s_tick
record_finalized_2min_candle = candle_timeline.record_finalized_2min_candle
seed_from_history            = candle_timeline.seed_from_history


# =============================================================================
# reversal-specific per-symbol filter results
# =============================================================================


# {SYMBOL: [{"id","label","passed","detail"}, ...]} -- last evaluation only.
_latest_filters: dict[str, List[dict]] = {}


def record_filter_results(symbol: str, results) -> None:
    """
    Store the last per-filter results for ``symbol``. ``results`` is the
    list of ``FilterResult`` tuples returned by
    ``filters.evaluate_filters``; we flatten each into a plain dict so
    the snapshot is JSON-serialisable without pulling the NamedTuple
    class into the viz layer.
    """
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
