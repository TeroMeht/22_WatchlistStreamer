"""
ORB long -- per-symbol overlay state for the dashboard.

Owns only the ORB-specific fields (yesterday high/low/close, latest
Rvol) and the shared overlay fields the dashboard renders on top of
the candle chart (reference line, fires, session state).

The candle timeline itself (5-sec ticks, finalized 2-min candles,
current in-progress candle, last bar time / close) lives in
``src.strategies.visualization.chart_state`` and is shared with every
other strategy's viz. This module re-exports the chart-side writers
below so existing callers (hooks, ``process_incoming_data``,
``datastreamer``) don't need to know that the storage moved.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from typing import List, Optional

from src.strategies.visualization import chart_state
# Yesterday's daily OHLC is owned by the strategy state (used by the
# yesterday-level filters); the dashboard reads it from there so we
# don't keep two copies in sync.
from src.strategies.orb_long import state as strategy_state

# ---- Re-exports from the shared candle timeline -----------------------------
# Callers that push into "the ORB viz" keep working unchanged; under the
# hood these all write to the shared per-symbol timeline.
record_5s_tick               = chart_state.record_5s_tick
record_finalized_2min_candle = chart_state.record_finalized_2min_candle
seed_from_history            = chart_state.seed_from_history

# Re-export shared state indicators so callers can do ``viz.STATE_BREAKOUT``.
STATE_WARMING_UP = chart_state.STATE_WARMING_UP
STATE_SEARCHING  = chart_state.STATE_SEARCHING
STATE_BREAKOUT   = chart_state.STATE_BREAKOUT
STATE_MUTED      = chart_state.STATE_MUTED

# Cap on the number of remembered ORB fires per symbol per session.
MAX_FIRES_PER_SYMBOL: int = 50


# =============================================================================
# ORB overlay state (per-symbol)
# =============================================================================


@dataclass
class OverlayState:
    symbol: str
    state: str = chart_state.STATE_WARMING_UP
    live_candle_count: int = 0
    ref_time: Optional[str] = None
    ref_close: Optional[float] = None
    ref_low: Optional[float] = None
    ref_field: Optional[str] = None   # "open" | "high" | "low" | "close"
    has_active_order: bool = False
    # Rvol of the most recent finalized 2-min candle; updated by finalize_candle.
    latest_rvol: Optional[float] = None
    fires: List[dict] = field(default_factory=list)
    updated_at: Optional[str] = None


_states: dict[str, OverlayState] = {}


def _overlay(symbol: str) -> OverlayState:
    key = symbol.upper()
    st = _states.get(key)
    if st is None:
        st = OverlayState(symbol=key)
        _states[key] = st
    return st


# =============================================================================
# Overlay writers (ORB-specific + strategy-agnostic overlay fields)
# =============================================================================


def record_reference(
    symbol: str,
    ref_time: dt_time,
    ref_close: float,
    ref_low: float,
    field: Optional[str] = None,
) -> None:
    st = _overlay(symbol)
    st.ref_time = ref_time.isoformat(timespec="seconds") if hasattr(ref_time, "isoformat") else str(ref_time)
    st.ref_close = float(ref_close)
    st.ref_low = float(ref_low)
    st.ref_field = field
    st.updated_at = chart_state.now_iso()


def record_state(symbol: str, state: str) -> None:
    st = _overlay(symbol)
    st.state = state
    st.updated_at = chart_state.now_iso()


def record_active_order(symbol: str, has_order: bool) -> None:
    st = _overlay(symbol)
    st.has_active_order = bool(has_order)
    st.updated_at = chart_state.now_iso()


def record_live_candle_count(symbol: str, count: int) -> None:
    st = _overlay(symbol)
    st.live_candle_count = int(count)
    st.updated_at = chart_state.now_iso()


def record_rvol(symbol: str, rvol: Optional[float]) -> None:
    """Update the latest 2-min candle's Rvol (called from finalize_candle)."""
    st = _overlay(symbol)
    st.latest_rvol = None if rvol is None else float(rvol)
    st.updated_at = chart_state.now_iso()


def record_fire(
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
    st = _overlay(symbol)
    interval = chart_state.to_2min_interval(bar_dt)
    st.fires.append({
        "ts": chart_state.to_unix_local_as_utc(interval),
        "t": bar_dt.replace(tzinfo=None).isoformat(timespec="seconds"),
        "c": float(close),
        "stop": float(stop_level) if stop_level is not None else None,
        "ref_close": float(ref_close),
    })
    if len(st.fires) > MAX_FIRES_PER_SYMBOL:
        st.fires = st.fires[-MAX_FIRES_PER_SYMBOL:]
    st.updated_at = chart_state.now_iso()


# =============================================================================
# Reader
# =============================================================================


def snapshot() -> dict:
    """
    Merged view: for every symbol with either overlay state OR candle
    data, produce one dict with ORB overlay fields + shared candle
    timeline fields.
    """
    all_syms = sorted(set(_states.keys()) | chart_state.known_symbols())
    symbols = []
    for sym in all_syms:
        st = _states.get(sym) or OverlayState(symbol=sym)
        view = chart_state.get_view(sym)
        symbols.append({
            "symbol":            st.symbol,
            "state":             st.state,
            "live_candle_count": st.live_candle_count,
            "ref_time":          st.ref_time,
            "ref_close":         st.ref_close,
            "ref_low":           st.ref_low,
            "ref_field":         st.ref_field,
            "has_active_order":  st.has_active_order,
            "last_bar_time":     view["last_bar_time"],
            "last_bar_close":    view["last_bar_close"],
            # Yesterday values read from the strategy-side store (single
            # source of truth; the filters gate on the same values).
            "yesterday_high":    strategy_state.yesterday_high(sym),
            "yesterday_close":   strategy_state.yesterday_close(sym),
            "latest_rvol":       st.latest_rvol,
            "candles":           view["candles"],
            "fires":             st.fires,
            "updated_at":        st.updated_at,
        })
    return {
        "generated_at": chart_state.now_iso(),
        "symbols": symbols,
    }


def reset() -> None:
    _states.clear()
