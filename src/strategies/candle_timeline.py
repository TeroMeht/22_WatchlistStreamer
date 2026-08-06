"""
Shared per-symbol candle timeline -- domain state, not viz state.

Every strategy operates on the SAME underlying 2-min candle series (they
all read from the same ``{symbol}_livestream`` table). Rather than
re-hitting the DB on every 5-sec tick or duplicating the tail per
strategy, we keep one in-memory timeline here that:

    * ``seed_from_history`` fills at streamer startup from the pre-loaded
      historical 2-min candles.
    * ``record_finalized_2min_candle`` appends to whenever
      ``finalize_candle`` writes a new row to the DB.
    * ``record_5s_tick`` updates the *current* in-progress 2-min candle
      from each live 5-sec bar plus tracks the last tick time / close.

Both strategy code (``breakout_level.get_reference_from_last_two_candles``)
and the viz layer read from here. The dependency arrow points
strategy_logic -> candle_timeline and viz -> candle_timeline; viz never
sits between the two.

Readers exposed:

    get_last_finalized_candles(symbol, n)
        Tail of finalized 2-min candles as dicts (dt/open/high/low/close).
        Cheap: list slice + tiny dict rebuild. Safe on the per-tick path.
    get_view(symbol) -> dict with keys candles, last_bar_time, last_bar_close
        The rendering-friendly view used by the dashboard.
    known_symbols() -> set of symbols with any candle data.

All writes are synchronous and cheap so they can safely live inside the
hot per-bar path. Everything runs on the streamer's asyncio loop, so no
locks are needed.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


# =============================================================================
# Helpers
# =============================================================================


def to_unix_local_as_utc(dt: datetime) -> int:
    """
    Convert a naive-or-aware wall-clock datetime to a Unix timestamp,
    treating the clock reading AS IF it were UTC. Lightweight Charts
    always formats timestamps as UTC on the axis, so we lie to it about
    the zone -- this way the axis reads the local wall clock the trader
    actually sees.
    """
    naive = dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
    return int(calendar.timegm(naive.timetuple()))


def to_2min_interval(dt: datetime) -> datetime:
    """Round a datetime down to the nearest 2-minute interval start."""
    naive = dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
    minute = (naive.minute // 2) * 2
    return naive.replace(second=0, microsecond=0, minute=minute)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# =============================================================================
# Shared constants
# =============================================================================

# Cap on the number of 2-min candles retained per symbol. 240 = 8 hours
# of 2-min candles -- comfortably covers a full RTH + extended-hours day.
MAX_CANDLES_PER_SYMBOL: int = 240


# =============================================================================
# Timeline store
# =============================================================================


@dataclass
class CandleTimeline:
    symbol: str
    last_bar_time: Optional[str] = None
    last_bar_close: Optional[float] = None
    candles_2min: List[dict] = field(default_factory=list)
    current_candle: Optional[dict] = None


_timelines: dict[str, CandleTimeline] = {}


def _get(symbol: str) -> CandleTimeline:
    key = symbol.upper()
    tl = _timelines.get(key)
    if tl is None:
        tl = CandleTimeline(symbol=key)
        _timelines[key] = tl
    return tl


# =============================================================================
# Writers
# =============================================================================


def record_5s_tick(
    symbol: str,
    bar_dt: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """Update the CURRENT (in-progress) 2-min candle from one 5-sec bar."""
    tl = _get(symbol)
    interval = to_2min_interval(bar_dt)
    interval_ts = to_unix_local_as_utc(interval)
    interval_iso = interval.isoformat(timespec="seconds")

    o, h, l, c = float(open_), float(high), float(low), float(close)

    cur = tl.current_candle
    if cur is None or cur["ts"] != interval_ts:
        tl.current_candle = {
            "ts": interval_ts,
            "t": interval_iso,
            "o": o, "h": h, "l": l, "c": c,
        }
    else:
        cur["h"] = max(cur["h"], h)
        cur["l"] = min(cur["l"], l)
        cur["c"] = c

    tl.last_bar_time = bar_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    tl.last_bar_close = c


def record_finalized_2min_candle(
    symbol: str,
    candle_dt: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """Append a completed 2-min OHLC candle. Idempotent for the tail row."""
    tl = _get(symbol)
    ts = to_unix_local_as_utc(candle_dt)
    iso = candle_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    candle = {
        "ts": ts, "t": iso,
        "o": float(open_), "h": float(high), "l": float(low), "c": float(close),
    }

    if tl.current_candle is not None and tl.current_candle["ts"] == ts:
        tl.current_candle = None

    if tl.candles_2min and tl.candles_2min[-1]["ts"] == ts:
        tl.candles_2min[-1] = candle
    elif tl.candles_2min and tl.candles_2min[-1]["ts"] > ts:
        return
    else:
        tl.candles_2min.append(candle)
        if len(tl.candles_2min) > MAX_CANDLES_PER_SYMBOL:
            tl.candles_2min = tl.candles_2min[-MAX_CANDLES_PER_SYMBOL:]


def seed_from_history(symbol: str, rows: list) -> None:
    """Bulk-seed the timeline with historical 2-min OHLC candles."""
    for row in rows:
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


# =============================================================================
# Readers
# =============================================================================


def get_view(symbol: str) -> dict:
    """
    Return the candle-related fields for ``symbol`` as a dict shaped for
    the dashboard. The current in-progress candle is appended to the
    ``candles`` list so the frontend sees a single flat series.
    """
    tl = _timelines.get(symbol.upper())
    if tl is None:
        return {"candles": [], "last_bar_time": None, "last_bar_close": None}
    candles = list(tl.candles_2min)
    if tl.current_candle is not None:
        # Never allow the current candle to reappear if it duplicates
        # the last finalized one (protects against a race between
        # finalize_candle and the next 5-sec tick).
        if not candles or candles[-1]["ts"] != tl.current_candle["ts"]:
            candles.append(tl.current_candle)
    return {
        "candles": candles,
        "last_bar_time": tl.last_bar_time,
        "last_bar_close": tl.last_bar_close,
    }


def known_symbols() -> set:
    """Set of symbols that have any candle data (historical or live)."""
    return set(_timelines.keys())


def get_last_finalized_candles(symbol: str, n: int) -> list[dict]:
    """
    Return the last ``n`` FINALIZED 2-min candles for ``symbol`` from
    the in-memory timeline (the same store that ``record_finalized_2min_candle``
    and ``seed_from_history`` write into). Each entry is a dict:

        {"dt": datetime, "open": float, "high": float,
         "low": float, "close": float}

    Returns an empty list if the timeline holds fewer than ``n`` candles.
    Cheap enough (list slice + tiny dict rebuild) to sit inside per-tick
    strategy paths -- callers should prefer this over hitting the DB for
    rolling-window references.
    """
    tl = _timelines.get(symbol.upper())
    if tl is None or len(tl.candles_2min) < n:
        return []
    return [
        {
            "dt": datetime.fromisoformat(c["t"]),
            "open": c["o"],
            "high": c["h"],
            "low":  c["l"],
            "close": c["c"],
        }
        for c in tl.candles_2min[-n:]
    ]


def reset() -> None:
    _timelines.clear()
