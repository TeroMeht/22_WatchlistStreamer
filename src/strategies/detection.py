"""
Shared breakout-detection primitive for entry strategies.

Stateless predicate: given the incoming live price and the level to
break, return a labelled ``BreakoutEvent``. The "don't re-fire this
session" concern is owned by each strategy's session latch
(``state.py``), not here.

Long-only for now. If a short-side strategy is added later, extend with
a ``side`` param and a symmetric ``price < level`` branch.
"""

from __future__ import annotations

from typing import NamedTuple


class BreakoutEvent(NamedTuple):
    is_breakout: bool
    reason: str  # human-readable string for logging


def detect_breakout(live_price: float, breakout_level: float) -> BreakoutEvent:
    """True when ``live_price`` is strictly above ``breakout_level``."""
    if live_price > breakout_level:
        return BreakoutEvent(
            True,
            f"BREAKOUT (incoming livestream price {live_price:.2f} > level {breakout_level:.2f})",
        )
    return BreakoutEvent(False, "no breakout detected")
