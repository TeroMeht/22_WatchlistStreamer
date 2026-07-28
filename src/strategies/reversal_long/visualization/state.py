"""
reversal_long -- per-symbol overlay state for the dashboard.

Owns only reversal-specific fields (rolling ``recent_max_relatr`` used
by the capitulation-check row) and the shared overlay fields (reference
line, fires, session state).

The candle timeline itself lives in
``src.strategies.visualization.chart_state`` and is shared across all
strategies. This module re-exports the chart-side writers below so
existing callers (hooks, ``process_incoming_data``, ``datastreamer``)
don't need to know that the storage moved.

Field diff vs the ORB overlay module:
    * Dropped ``yesterday_high`` / ``yesterday_low`` / ``yesterday_close``
      / ``latest_rvol`` -- ORB-specific.
    * Added ``recent_max_relatr`` -- max ``Relatr`` across the last
      ``RECENT_RELATR_WINDOW`` finalized 2-min candles. The dashboard
      compares it to ``settings.CAPITULATION_THRESHOLD`` to render the
      "recent capitulation" setup check.
    * ``snapshot()`` also exposes ``capitulation_threshold`` and
      ``recent_relatr_window`` at the top level so the dashboard
      doesn't need to hardcode them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from typing import List, Optional

from src.core.config import settings
from src.strategies.visualization import chart_state

# ---- Re-exports from the shared candle timeline -----------------------------
record_5s_tick               = chart_state.record_5s_tick
record_finalized_2min_candle = chart_state.record_finalized_2min_candle
seed_from_history            = chart_state.seed_from_history

STATE_WARMING_UP = chart_state.STATE_WARMING_UP
STATE_SEARCHING  = chart_state.STATE_SEARCHING
STATE_BREAKOUT   = chart_state.STATE_BREAKOUT
STATE_MUTED      = chart_state.STATE_MUTED

# Cap on the number of remembered fires per symbol per session.
MAX_FIRES_PER_SYMBOL: int = 50

# How many recent 2-min Relatr values feed ``recent_max_relatr``. Must
# match ``filters.filters.CAPITULATION_LOOKBACK_CANDLES`` so the
# dashboard shows the same window the strategy actually gates on.
RECENT_RELATR_WINDOW: int = 3


# =============================================================================
# reversal_long overlay state (per-symbol)
# =============================================================================


@dataclass
class OverlayState:
    symbol: str
    state: str = chart_state.STATE_WARMING_UP
    live_candle_count: int = 0
    ref_time: Optional[str] = None
    ref_close: Optional[float] = None
    ref_low: Optional[float] = None
    ref_field: Optional[str] = None
    has_active_order: bool = False
    # Rolling max Relatr across the last RECENT_RELATR_WINDOW finalized
    # 2-min candles. Compared against CAPITULATION_THRESHOLD in the
    # dashboard for the "recent capitulation" setup check.
    recent_max_relatr: Optional[float] = None
    fires: List[dict] = field(default_factory=list)
    updated_at: Optional[str] = None


_states: dict[str, OverlayState] = {}
_recent_relatrs: dict[str, List[float]] = {}


def _overlay(symbol: str) -> OverlayState:
    key = symbol.upper()
    st = _states.get(key)
    if st is None:
        st = OverlayState(symbol=key)
        _states[key] = st
    return st


# =============================================================================
# Overlay writers
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


def record_relatr(symbol: str, relatr: Optional[float]) -> None:
    """
    Push the latest 2-min Relatr. Maintains a rolling buffer of the last
    ``RECENT_RELATR_WINDOW`` values so ``recent_max_relatr`` reflects the
    same lookback the strategy filter gates on.
    """
    if relatr is None:
        return
    st = _overlay(symbol)
    buf = _recent_relatrs.setdefault(symbol.upper(), [])
    buf.append(float(relatr))
    if len(buf) > RECENT_RELATR_WINDOW:
        del buf[:-RECENT_RELATR_WINDOW]
    st.recent_max_relatr = max(buf)
    st.updated_at = chart_state.now_iso()


def record_fire(
    symbol: str,
    bar_dt: datetime,
    close: float,
    stop_level: Optional[float],
    ref_close: float,
) -> None:
    """
    Log a breakout fire. ``stop_level`` may be ``None`` for the
    alarm-only MVP; the dashboard checks for null before rendering a
    stop line.
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
    """Merged view: reversal_long overlays + shared candle timeline."""
    all_syms = sorted(set(_states.keys()) | chart_state.known_symbols())
    symbols = []
    for sym in all_syms:
        st = _states.get(sym) or OverlayState(symbol=sym)
        view = chart_state.get_view(sym)
        symbols.append({
            "symbol":             st.symbol,
            "state":              st.state,
            "live_candle_count":  st.live_candle_count,
            "ref_time":           st.ref_time,
            "ref_close":          st.ref_close,
            "ref_low":            st.ref_low,
            "ref_field":          st.ref_field,
            "has_active_order":   st.has_active_order,
            "last_bar_time":      view["last_bar_time"],
            "last_bar_close":     view["last_bar_close"],
            "recent_max_relatr":  st.recent_max_relatr,
            "candles":            view["candles"],
            "fires":              st.fires,
            "updated_at":         st.updated_at,
        })
    return {
        "generated_at": chart_state.now_iso(),
        "capitulation_threshold": float(settings.CAPITULATION_THRESHOLD),
        "recent_relatr_window": RECENT_RELATR_WINDOW,
        "symbols": symbols,
    }


def reset() -> None:
    _states.clear()
    _recent_relatrs.clear()
