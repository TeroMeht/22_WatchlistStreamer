"""
Shared per-strategy overlay state -- dataclass + keyed store.

Every entry strategy's dashboard card carries the same core plumbing:

    * one ``OverlayState`` per symbol (reference line + updated_at)
    * one bounded list of fire records per symbol
    * a snapshot builder that merges the above with the shared candle
      timeline

The dataclass defines the shape; the module-level store (keyed by a
per-strategy identifier) holds one slice per strategy. Viz modules keep
only the fields that are truly strategy-specific (e.g. ``latest_rvol``
for ORB, ``recent_max_relatr`` for reversal) plus their strategy-specific
snapshot decorations.

Conventions:
    * ``strategy_key`` is a short identifier picked by each strategy's
      viz module (e.g. ``"orb"``, ``"reversal"``). It's opaque -- just
      needs to be unique across strategies so the keyed dicts don't
      collide.
    * All writes bump ``OverlayState.updated_at`` via ``_touch`` so a
      strategy-specific writer can call ``touch(...)`` to bump it too
      without pulling in the whole store shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

from src.strategies import candle_timeline


# =============================================================================
# Shape
# =============================================================================


@dataclass
class OverlayState:
    symbol: str
    ref_time: Optional[str] = None
    ref_close: Optional[float] = None
    ref_low: Optional[float] = None
    ref_field: Optional[str] = None   # "open" | "high" | "low" | "close"
    updated_at: Optional[str] = None


# Cap on the number of remembered fires per (strategy, symbol) per session.
MAX_FIRES_PER_SYMBOL: int = 50


# =============================================================================
# Keyed stores
# =============================================================================


# strategy_key -> { symbol -> OverlayState }
_states: dict[str, dict[str, OverlayState]] = {}
# strategy_key -> { symbol -> list[fire dict] }
_fires:  dict[str, dict[str, List[dict]]] = {}


def _overlay(strategy_key: str, symbol: str) -> OverlayState:
    per_strat = _states.setdefault(strategy_key, {})
    key = symbol.upper()
    st = per_strat.get(key)
    if st is None:
        st = OverlayState(symbol=key)
        per_strat[key] = st
    return st


def _touch(strategy_key: str, symbol: str) -> None:
    _overlay(strategy_key, symbol).updated_at = candle_timeline.now_iso()


# =============================================================================
# Writers
# =============================================================================


def record_reference(
    strategy_key: str,
    symbol: str,
    ref_time,
    ref_close: float,
    ref_low: float,
    field: Optional[str] = None,
) -> None:
    st = _overlay(strategy_key, symbol)
    st.ref_time = ref_time.isoformat(timespec="seconds") if hasattr(ref_time, "isoformat") else str(ref_time)
    st.ref_close = float(ref_close)
    st.ref_low = float(ref_low)
    st.ref_field = field
    st.updated_at = candle_timeline.now_iso()


def record_fire(
    strategy_key: str,
    symbol: str,
    bar_dt: datetime,
    close: float,
    stop_level: Optional[float],
    ref_close: float,
) -> None:
    """
    Log a breakout fire. Marker time is snapped to the enclosing 2-min
    interval so it aligns cleanly with the candle it triggered inside of.
    ``stop_level`` may be ``None`` (alarm-only strategies) -- the
    dashboard checks for null before rendering a stop line for the fire.
    """
    per_strat = _fires.setdefault(strategy_key, {})
    key = symbol.upper()
    interval = candle_timeline.to_2min_interval(bar_dt)
    buf = per_strat.setdefault(key, [])
    buf.append({
        "ts": candle_timeline.to_unix_local_as_utc(interval),
        "t": bar_dt.replace(tzinfo=None).isoformat(timespec="seconds"),
        "c": float(close),
        "stop": float(stop_level) if stop_level is not None else None,
        "ref_close": float(ref_close),
    })
    if len(buf) > MAX_FIRES_PER_SYMBOL:
        del buf[:-MAX_FIRES_PER_SYMBOL]
    _touch(strategy_key, symbol)


def touch(strategy_key: str, symbol: str) -> None:
    """
    Bump ``updated_at`` for the overlay row. Called by strategy-specific
    writers (e.g. ``record_filter_results``) whose data lives outside
    this module but should still refresh the dashboard timestamp.
    """
    _touch(strategy_key, symbol)


# =============================================================================
# Reader
# =============================================================================


def snapshot(
    strategy_key: str,
    extra_symbol_fields: Optional[Callable[[str], dict]] = None,
) -> dict:
    """
    Produce the shared shape of a strategy's dashboard snapshot.

    Emits one entry per symbol that either (a) has any overlay/fires
    data for this strategy or (b) has any candle data in the shared
    timeline. Fields per entry: ``symbol``, ``ref_time``, ``ref_close``,
    ``ref_low``, ``ref_field``, ``last_bar_time``, ``last_bar_close``,
    ``candles``, ``fires``, ``updated_at``.

    ``extra_symbol_fields(symbol)`` is called once per symbol and its
    return dict is merged into that symbol's entry -- strategies use it
    to layer in ``latest_rvol`` / ``recent_max_relatr`` / etc without
    building their own snapshot loop.
    """
    strat_states = _states.get(strategy_key, {})
    strat_fires  = _fires.get(strategy_key, {})
    all_syms = sorted(set(strat_states.keys()) | candle_timeline.known_symbols())

    symbols = []
    for sym in all_syms:
        st = strat_states.get(sym) or OverlayState(symbol=sym)
        view = candle_timeline.get_view(sym)
        entry = {
            "symbol":         st.symbol,
            "ref_time":       st.ref_time,
            "ref_close":      st.ref_close,
            "ref_low":        st.ref_low,
            "ref_field":      st.ref_field,
            "last_bar_time":  view["last_bar_time"],
            "last_bar_close": view["last_bar_close"],
            "candles":        view["candles"],
            "fires":          list(strat_fires.get(sym, [])),
            "updated_at":     st.updated_at,
        }
        if extra_symbol_fields is not None:
            entry.update(extra_symbol_fields(sym))
        symbols.append(entry)

    return {
        "generated_at": candle_timeline.now_iso(),
        "symbols": symbols,
    }


def reset(strategy_key: Optional[str] = None) -> None:
    """
    Reset the store. Without a ``strategy_key`` clears every strategy's
    state and fires; with one clears only that strategy's slice.
    """
    if strategy_key is None:
        _states.clear()
        _fires.clear()
        return
    _states.pop(strategy_key, None)
    _fires.pop(strategy_key, None)
