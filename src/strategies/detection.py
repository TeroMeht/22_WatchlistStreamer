"""
Shared breakout / breakdown detection primitive for entry strategies.

Stateless predicate: given the incoming live price, a level, and a
direction, return a labelled ``BreakoutEvent``. The "don't re-fire this
session" concern is owned by each strategy's session latch, not here.

Direction is one of:
    * ``"long"``  -- fires when ``live_price >  level`` (break UP)
    * ``"short"`` -- fires when ``live_price <  level`` (break DOWN)
"""

from __future__ import annotations

from typing import NamedTuple


class BreakoutEvent(NamedTuple):
    is_breakout: bool
    reason: str  # human-readable string for logging


def detect_breakout(
    live_price: float,
    breakout_level: float,
    direction: str = "long",
) -> BreakoutEvent:
    """
    True when the current live price has crossed ``breakout_level`` in
    the direction of the trade:

        long  -> live_price >  breakout_level
        short -> live_price <  breakout_level
    """
    direction = direction.lower()
    if direction == "long":
        if live_price > breakout_level:
            return BreakoutEvent(
                True,
                f"BREAKOUT (incoming livestream price {live_price:.2f} > level {breakout_level:.2f})",
            )
        return BreakoutEvent(False, "no breakout detected")
    if direction == "short":
        if live_price < breakout_level:
            return BreakoutEvent(
                True,
                f"BREAKDOWN (incoming livestream price {live_price:.2f} < level {breakout_level:.2f})",
            )
        return BreakoutEvent(False, "no breakdown detected")
    raise ValueError(f"Unsupported direction: {direction!r}")
