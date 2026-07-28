"""
In-memory ORB visualization state.

The dashboard server reads a snapshot from here every ~1 s. Writes come
from three places:

    * At streamer startup, ``seed_from_history`` populates the finished
      2-min candles from the pre-loaded ``{symbol}_livestream`` table.
    * On every 5-sec bar, ``orb_breakout_long`` calls ``record_5s_tick``
      which updates the *current* (still-forming) 2-min candle in place
      so the chart shows a growing candle as ticks arrive.
    * Each time ``finalize_candle`` writes a 2-min row to the DB, we call
      ``record_finalized_2min_candle`` to append the completed candle to
      the series and clear the "current" candle.

All writes are synchronous and cheap so they can safely live inside the
hot per-bar path. Everything runs on the streamer's asyncio loop, so no
locks are needed.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from typing import List, Optional


def _to_unix_local_as_utc(dt: datetime) -> int:
    """
    Convert a naive-or-aware wall-clock datetime to a Unix timestamp,
    treating the clock reading AS IF it were UTC. Lightweight Charts
    always formats timestamps as UTC on the axis, so we lie to it about
    the zone -- this way the axis reads the local wall clock the trader
    actually sees.
    """
    naive = dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
    return int(calendar.timegm(naive.timetuple()))


def _to_2min_interval(dt: datetime) -> datetime:
    """Round a datetime down to the nearest 2-minute interval start."""
    naive = dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
    minute = (naive.minute // 2) * 2
    return naive.replace(second=0, microsecond=0, minute=minute)


# Cap on the number of 2-min candles retained per symbol. 240 = 8 hours
# of 2-min candles -- comfortably covers a full RTH + extended-hours day.
MAX_CANDLES_PER_SYMBOL: int = 240

# Cap on the number of remembered ORB fires per symbol per session.
MAX_FIRES_PER_SYMBOL: int = 50


# Distinguishable states the dashboard renders.
STATE_WARMING_UP: str = "warming_up"
STATE_SEARCHING: str = "searching"
STATE_BREAKOUT: str = "breakout"
STATE_MUTED: str = "muted"


@dataclass
class SymbolState:
    symbol: str
    state: str = STATE_WARMING_UP
    live_candle_count: int = 0
    ref_time: Optional[str] = None
    ref_close: Optional[float] = None
    ref_low: Optional[float] = None
    ref_field: Optional[str] = None   # "open" | "high" | "low" | "close"
    has_active_order: bool = False
    last_bar_time: Optional[str] = None
    last_bar_close: Optional[float] = None
    # Setup-validation inputs. RTH-only (from useRTH=True daily fetch), so
    # premarket is naturally excluded from yesterday_high / yesterday_close.
    yesterday_high: Optional[float] = None
    yesterday_close: Optional[float] = None
    # Rvol of the most recent finalized 2-min candle; updated by
    # finalize_candle each time a new candle is written to the DB.
    latest_rvol: Optional[float] = None
    # Finalized 2-min candles (historical seed + live-inserted).
    candles_2min: List[dict] = field(default_factory=list)
    # In-progress 2-min candle, updated as 5-sec ticks come in. None until
    # the first live 5-sec bar arrives for the next interval boundary.
    current_candle: Optional[dict] = None
    fires: List[dict] = field(default_factory=list)
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        # Flatten to a single "candles" list so the frontend does not
        # need to know about the historical / current split. Current
        # candle (if any) is appended AFTER the finalized list.
        candles = list(self.candles_2min)
        if self.current_candle is not None:
            # Never allow the current candle to reappear if it duplicates
            # the last finalized one (protects against a race between
            # finalize_candle and the next 5-sec tick).
            if not candles or candles[-1]["ts"] != self.current_candle["ts"]:
                candles.append(self.current_candle)
        return {
            "symbol": self.symbol,
            "state": self.state,
            "live_candle_count": self.live_candle_count,
            "ref_time": self.ref_time,
            "ref_close": self.ref_close,
            "ref_low": self.ref_low,
            "ref_field": self.ref_field,
            "has_active_order": self.has_active_order,
            "last_bar_time": self.last_bar_time,
            "last_bar_close": self.last_bar_close,
            "yesterday_high": self.yesterday_high,
            "yesterday_close": self.yesterday_close,
            "latest_rvol": self.latest_rvol,
            "candles": candles,
            "fires": self.fires,
            "updated_at": self.updated_at,
        }


_states: dict[str, SymbolState] = {}


def _get(symbol: str) -> SymbolState:
    key = symbol.upper()
    st = _states.get(key)
    if st is None:
        st = SymbolState(symbol=key)
        _states[key] = st
    return st


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def record_5s_tick(
    symbol: str,
    bar_dt: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """
    Update the CURRENT (in-progress) 2-min candle from one 5-sec bar.
    Called on every live 5-sec bar. The current candle animates in
    place on the chart until ``record_finalized_2min_candle`` closes
    it out at the 2-min boundary.
    """
    st = _get(symbol)
    interval = _to_2min_interval(bar_dt)
    interval_ts = _to_unix_local_as_utc(interval)
    interval_iso = interval.isoformat(timespec="seconds")

    o, h, l, c = float(open_), float(high), float(low), float(close)

    cur = st.current_candle
    if cur is None or cur["ts"] != interval_ts:
        # New interval -- start a fresh in-progress candle.
        st.current_candle = {
            "ts": interval_ts,
            "t": interval_iso,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
        }
    else:
        cur["h"] = max(cur["h"], h)
        cur["l"] = min(cur["l"], l)
        cur["c"] = c

    st.last_bar_time = bar_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    st.last_bar_close = c
    st.updated_at = _now_iso()


def record_finalized_2min_candle(
    symbol: str,
    candle_dt: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """
    Append a completed 2-min OHLC candle. Used for both the historical
    seed at startup and for candles finalized during the live session.
    Idempotent: a duplicate timestamp overwrites the existing row so
    calling this from both the seed loop and a later finalize_candle
    won't produce dupes.
    """
    st = _get(symbol)
    ts = _to_unix_local_as_utc(candle_dt)
    iso = candle_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    candle = {
        "ts": ts,
        "t": iso,
        "o": float(open_),
        "h": float(high),
        "l": float(low),
        "c": float(close),
    }

    # Clear the in-progress candle if it was for this same interval.
    if st.current_candle is not None and st.current_candle["ts"] == ts:
        st.current_candle = None

    # Idempotent insert: replace if same timestamp already at tail,
    # ignore if it would move backwards, otherwise append.
    if st.candles_2min and st.candles_2min[-1]["ts"] == ts:
        st.candles_2min[-1] = candle
    elif st.candles_2min and st.candles_2min[-1]["ts"] > ts:
        return
    else:
        st.candles_2min.append(candle)
        if len(st.candles_2min) > MAX_CANDLES_PER_SYMBOL:
            st.candles_2min = st.candles_2min[-MAX_CANDLES_PER_SYMBOL:]

    st.updated_at = _now_iso()


def record_reference(
    symbol: str,
    ref_time: dt_time,
    ref_close: float,
    ref_low: float,
    field: Optional[str] = None,
) -> None:
    st = _get(symbol)
    st.ref_time = ref_time.isoformat(timespec="seconds") if hasattr(ref_time, "isoformat") else str(ref_time)
    st.ref_close = float(ref_close)
    st.ref_low = float(ref_low)
    st.ref_field = field
    st.updated_at = _now_iso()


def record_state(symbol: str, state: str) -> None:
    st = _get(symbol)
    st.state = state
    st.updated_at = _now_iso()


def record_active_order(symbol: str, has_order: bool) -> None:
    st = _get(symbol)
    st.has_active_order = bool(has_order)
    st.updated_at = _now_iso()


def record_live_candle_count(symbol: str, count: int) -> None:
    st = _get(symbol)
    st.live_candle_count = int(count)
    st.updated_at = _now_iso()


def record_yesterday_daily(symbol: str, high: float, close: float) -> None:
    """Seed yesterday's RTH high and close for the setup-validation checks."""
    st = _get(symbol)
    st.yesterday_high = float(high)
    st.yesterday_close = float(close)
    st.updated_at = _now_iso()


def record_rvol(symbol: str, rvol: Optional[float]) -> None:
    """Update the latest 2-min candle's Rvol (called from finalize_candle)."""
    st = _get(symbol)
    st.latest_rvol = None if rvol is None else float(rvol)
    st.updated_at = _now_iso()


def record_fire(
    symbol: str,
    bar_dt: datetime,
    close: float,
    stop_level: float,
    ref_close: float,
) -> None:
    """
    Log a breakout fire. Marker time is snapped to the enclosing 2-min
    interval so it aligns cleanly with the candle it triggered inside of.
    """
    st = _get(symbol)
    interval = _to_2min_interval(bar_dt)
    st.fires.append({
        "ts": _to_unix_local_as_utc(interval),
        "t": bar_dt.replace(tzinfo=None).isoformat(timespec="seconds"),
        "c": float(close),
        "stop": float(stop_level),
        "ref_close": float(ref_close),
    })
    if len(st.fires) > MAX_FIRES_PER_SYMBOL:
        st.fires = st.fires[-MAX_FIRES_PER_SYMBOL:]
    st.updated_at = _now_iso()


# ---------------------------------------------------------------------------
# Historical seed helper -- called once from run_streamer after the
# `{symbol}_livestream` tables have been populated with intraday data.
# ---------------------------------------------------------------------------


def seed_from_history(symbol: str, rows: list) -> None:
    """
    Bulk-seed the visualization with historical 2-min OHLC candles.

    ``rows`` is an iterable of objects with attributes / keys
    ``Date`` (date), ``Time`` (time), ``Open``, ``High``, ``Low``, ``Close``.
    Accepts pandas DataFrame rows, dicts, or plain objects.
    """
    for row in rows:
        # Duck-type: support pandas Series (dict-like) and namedtuple-like.
        def _get_field(name):
            if hasattr(row, "get") and callable(getattr(row, "get")):
                v = row.get(name)
                if v is not None:
                    return v
            return getattr(row, name, None) or row[name]

        d = _get_field("Date")
        t = _get_field("Time")
        candle_dt = datetime.combine(d, t)
        record_finalized_2min_candle(
            symbol=symbol,
            candle_dt=candle_dt,
            open_=float(_get_field("Open")),
            high=float(_get_field("High")),
            low=float(_get_field("Low")),
            close=float(_get_field("Close")),
        )


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


def snapshot() -> dict:
    return {
        "generated_at": _now_iso(),
        "symbols": [st.to_dict() for st in sorted(_states.values(), key=lambda s: s.symbol)],
    }


def reset() -> None:
    _states.clear()
