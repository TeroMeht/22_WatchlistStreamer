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
    # Iso of the last 5-sec bar we accumulated volume for. Guards
    # against double-counting when the same bar reaches
    # ``record_5s_tick`` from more than one caller (see module
    # docstring). ``None`` = no tick recorded yet this session.
    last_tick_bar_dt: Optional[str] = None


_timelines: dict[str, CandleTimeline] = {}


def _get(symbol: str) -> CandleTimeline:
    key = symbol.upper()
    tl = _timelines.get(key)
    if tl is None:
        tl = CandleTimeline(symbol=key)
        _timelines[key] = tl
    return tl


def _clean_float(v) -> Optional[float]:
    """
    Coerce a numeric-ish value to float, returning ``None`` for ``None``,
    ``NaN``, or anything that can't be cast. Used for the nullable
    indicator fields (vwap / ema9) where a missing value is legitimate
    on the very first candles of the session.
    """
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


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
    volume: Optional[float] = None,
) -> None:
    """
    Update the CURRENT (in-progress) 2-min candle from one 5-sec bar.

    ``volume`` (optional) is the 5-sec bar's volume; it accumulates into
    the current 2-min candle's ``v`` field. Duplicate calls for the same
    bar (identified by ``bar_dt``) are safe: OHLC is idempotent and the
    volume add is skipped on repeats via ``last_tick_bar_dt``.
    """
    tl = _get(symbol)
    interval = to_2min_interval(bar_dt)
    interval_ts = to_unix_local_as_utc(interval)
    interval_iso = interval.isoformat(timespec="seconds")

    o, h, l, c = float(open_), float(high), float(low), float(close)
    bar_key = bar_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    # A repeat call for the same 5-sec bar (see module docstring):
    # OHLC is last-write-wins, but volume must NOT accumulate again.
    is_new_bar = (tl.last_tick_bar_dt != bar_key)
    v = float(volume) if (volume is not None and is_new_bar) else 0.0

    cur = tl.current_candle
    if cur is None or cur["ts"] != interval_ts:
        tl.current_candle = {
            "ts": interval_ts,
            "t": interval_iso,
            "o": o, "h": h, "l": l, "c": c,
            # In-progress volume: seed with this bar's contribution (or 0
            # if volume wasn't supplied). Finalize replaces it with the
            # authoritative DB value via ``record_finalized_2min_candle``.
            "v": v,
            # 5-sec bars carry no vwap / ema9 -- the dashboard just ends
            # its VWAP / EMA9 line series at the last finalized candle.
            "vwap": None, "ema9": None,
        }
    else:
        cur["h"] = max(cur["h"], h)
        cur["l"] = min(cur["l"], l)
        cur["c"] = c
        if is_new_bar:
            cur["v"] = float(cur.get("v") or 0.0) + v

    tl.last_bar_time = bar_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    tl.last_bar_close = c
    tl.last_tick_bar_dt = bar_key


def record_finalized_2min_candle(
    symbol: str,
    candle_dt: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: Optional[float] = None,
    vwap: Optional[float] = None,
    ema9: Optional[float] = None,
) -> None:
    """
    Append a completed 2-min OHLC candle. Idempotent for the tail row.

    ``volume`` is the authoritative aggregated volume from the DB row
    (or the historical seed). It replaces whatever the in-progress
    tick-accumulated value was for this interval.

    ``vwap`` / ``ema9`` are the finalize-time indicator values (see
    ``SymbolSessionState.apply_bar``). They ride along on the candle dict so
    the dashboard can plot them as overlay line series without needing
    a second store.
    """
    tl = _get(symbol)
    ts = to_unix_local_as_utc(candle_dt)
    iso = candle_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    candle = {
        "ts": ts, "t": iso,
        "o": float(open_), "h": float(high), "l": float(low), "c": float(close),
        "v": _clean_float(volume),
        "vwap": _clean_float(vwap),
        "ema9": _clean_float(ema9),
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

    # Populate last-bar fields from the finalized candle too so
    # dashboards for candle-driven strategies (vwap_continuation_long)
    # have something to display even when no realtime strategy is
    # arming ``record_5s_tick`` on this symbol. Only advance in the
    # forward direction so a live 5-sec tick that arrived first for
    # a later interval doesn't get overwritten by a finalize catching
    # up to a stale row.
    if tl.last_bar_time is None or iso >= tl.last_bar_time:
        tl.last_bar_time = iso
        tl.last_bar_close = float(close)


def seed_from_history(symbol: str, rows: list) -> None:
    """Bulk-seed the timeline with historical 2-min OHLC candles."""
    for row in rows:
        def _get_field(name):
            if hasattr(row, "get") and callable(getattr(row, "get")):
                v = row.get(name)
                if v is not None:
                    return v
            return getattr(row, name, None) or row[name]

        def _get_optional(name):
            """Same as _get_field but returns None on missing/exception."""
            try:
                if hasattr(row, "get") and callable(getattr(row, "get")):
                    return row.get(name)
                return getattr(row, name, None)
            except Exception:
                return None

        d = _get_field("date")
        t = _get_field("time")
        candle_dt = datetime.combine(d, t)
        # Historical rows come from the livestream table (all-caps VWAP /
        # EMA9 columns). Older/alternate frames may use title-cased
        # ``Vwap`` / ``Ema9`` -- try both so seeding is robust to either.
        vwap = _get_optional("vwap")
        if vwap is None:
            vwap = _get_optional("Vwap")
        ema9 = _get_optional("ema9")
        if ema9 is None:
            ema9 = _get_optional("Ema9")
        volume = _get_optional("volume")
        if volume is None:
            volume = _get_optional("Volume")
        record_finalized_2min_candle(
            symbol=symbol,
            candle_dt=candle_dt,
            open_=float(_get_field("open")),
            high=float(_get_field("high")),
            low=float(_get_field("low")),
            close=float(_get_field("close")),
            volume=volume,
            vwap=vwap,
            ema9=ema9,
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
