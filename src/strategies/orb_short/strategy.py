"""
ORB short (breakdown) -- entry point for the realtime dispatch loop.

Thin wrapper around the shared ``ORBStrategy`` class in
``src.strategies.orb_shared.strategy_class``. Mirrors ``orb_long`` with
``direction="short"``: detection is reversed (price must break BELOW
the opening-range LOW) and the stop is anchored ABOVE the running
session high. Filters are NOT shared with ORB long -- this instance
wires its own ``.filters.filters`` module with the short-side gates
(price below premarket low, yesterday low, yesterday close).
"""

from __future__ import annotations

from src.strategies.orb_shared.strategy_class import ORBStrategy

from .filters import filters as short_filters
from .visualization import state as viz


# Signal name used in the alarm row + Telegram message when this fires.
ORB_SHORT_SIGNAL_NAME: str = "orb_breakdown"


_impl = ORBStrategy(
    direction="short",
    signal_name=ORB_SHORT_SIGNAL_NAME,
    viz=viz,
    filters=short_filters,
)


async def orb_breakdown(bar, symbol: str) -> None:
    """Realtime entry hook -- delegates to the shared ORB orchestrator."""
    await _impl.run(bar, symbol)
