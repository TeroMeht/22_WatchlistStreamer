"""
ORB arming: turn a completed opening range into ONE order request for the
26 executor, which places it at IB as a STP LMT + protective STP. IB then
triggers it on its own servers at the exact breakout tick.

The ORB filters are all of the form "price beyond X", so instead of
checking them on every bar they are folded into the trigger price:

    long : trigger = max(OR high, premarket high, yesterday high) + tick
           -> at the trigger, price > yhi, > yclose (yhi >= yclose) and
              >= pm high all hold automatically
    short: trigger = min(OR low, premarket low, yesterday low) - tick

    stop  = opposite OR extreme -/+ ORB_STOP_OFFSET   (same as today)
    limit = trigger +/- ORB_LIMIT_OFFSET_R * R          (R = |trigger - stop|)

No entry cutoff: the order is a DAY order at IB and stays working until
it fills, is cancelled, or the session ends.

build_arm_plan() is pure (no I/O) so it can be unit-tested.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Tuple

import pandas as pd

from src.core.config import settings
from src.database.db_functions import get_last_rows


@dataclass(frozen=True)
class ArmPlan:
    symbol: str
    direction: str
    trigger: float
    limit: float
    stop: float
    parts: dict = field(default_factory=dict)   # inputs, for logging

    def payload(self) -> dict:
        """Body for POST /api/executor/arm. No valid_until -> DAY order."""
        return {
            "symbol": self.symbol,
            "strategy": "ORB",
            "direction": self.direction,
            "trigger": self.trigger,
            "limit": self.limit,
            "stop": self.stop,
        }


def build_arm_plan(
    *,
    symbol: str,
    direction: str,
    day: date,
    or_high: float,
    or_low: float,
    pm_high: Optional[float],
    pm_low: Optional[float],
    y_high: Optional[float],
    y_low: Optional[float],
    tick: float,
    stop_offset: float,
    limit_offset_r: float,
) -> Tuple[Optional[ArmPlan], str]:
    """Returns (plan, "") or (None, reason-not-to-arm)."""
    if direction == "long":
        # Same inputs the long filters need; missing input = filter would fail.
        if y_high is None:
            return None, "yesterday high not seeded"
        if pm_high is None:
            return None, "no premarket candles"
        trigger = round(max(or_high, pm_high, y_high) + tick, 2)
        stop = round(or_low - stop_offset, 2)
        r = trigger - stop
        limit = round(trigger + limit_offset_r * r, 2)
    elif direction == "short":
        if y_low is None:
            return None, "yesterday low not seeded"
        if pm_low is None:
            return None, "no premarket candles"
        trigger = round(min(or_low, pm_low, y_low) - tick, 2)
        stop = round(or_high + stop_offset, 2)
        r = stop - trigger
        limit = round(trigger - limit_offset_r * r, 2)
    else:
        raise ValueError(f"Unsupported direction: {direction!r}")

    if r <= 0:
        return None, f"non-positive risk (trigger={trigger}, stop={stop})"

    plan = ArmPlan(
        symbol=symbol.upper(),
        direction=direction,
        trigger=trigger,
        limit=limit,
        stop=stop,
        parts={
            "or_high": or_high, "or_low": or_low,
            "pm_high": pm_high, "pm_low": pm_low,
            "y_high": y_high, "y_low": y_low,
            "R": round(r, 4),
        },
    )
    return plan, ""


async def get_premarket_extremes(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """(premarket high, premarket low) from the 2-min livestream rows strictly
    before SESSION_START -- the same rows the pm_high / pm_low filters use."""
    df = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=None)
    if df is None or df.empty:
        return None, None
    pre: pd.DataFrame = df[df["time"] < settings.SESSION_START]
    if pre.empty:
        return None, None
    return float(pre["high"].max()), float(pre["low"].min())
