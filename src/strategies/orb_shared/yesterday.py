from __future__ import annotations

from typing import Optional


_yesterday_daily: dict[str, dict[str, float]] = {}


def record_yesterday_daily(
    symbol: str, high: float, low: float, close: float,
) -> None:
    """Cache yesterday's RTH high, low, and close for downstream filters."""
    _yesterday_daily[symbol.upper()] = {
        "high":  float(high),
        "low":   float(low),
        "close": float(close),
    }


def yesterday_high(symbol: str) -> Optional[float]:
    d = _yesterday_daily.get(symbol.upper())
    return d["high"] if d else None


def yesterday_low(symbol: str) -> Optional[float]:
    d = _yesterday_daily.get(symbol.upper())
    return d["low"] if d else None


def yesterday_close(symbol: str) -> Optional[float]:
    d = _yesterday_daily.get(symbol.upper())
    return d["close"] if d else None
