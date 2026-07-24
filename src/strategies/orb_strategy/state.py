"""
Session-scoped module state for the ORB long strategy.

Currently holds yesterday's RTH high and close per symbol, seeded once
at streamer startup from the useRTH=True daily fetch. Read by the
yesterday-level filters. Premarket is naturally excluded because the
daily bar covers regular hours only.

Reset semantics: process-scoped. A streamer restart wipes the cache
and it gets re-seeded from the fresh daily fetch during startup.
"""

from __future__ import annotations

from typing import Optional

from src.visualization import orb_state as viz



# --- Yesterday's RTH high / close --------------------------------------------
# Seeded once at streamer startup from the useRTH=True daily fetch. Read by
# the yesterday-level filters. Premarket is naturally excluded because the
# daily bar covers regular hours only.
_yesterday_daily: dict[str, dict[str, float]] = {}


def record_yesterday_daily(symbol: str, high: float, close: float) -> None:
    """Cache yesterday's RTH high and close for downstream filter checks."""
    _yesterday_daily[symbol.upper()] = {"high": float(high), "close": float(close)}


def yesterday_high(symbol: str) -> Optional[float]:
    d = _yesterday_daily.get(symbol.upper())
    return d["high"] if d else None


def yesterday_close(symbol: str) -> Optional[float]:
    d = _yesterday_daily.get(symbol.upper())
    return d["close"] if d else None
