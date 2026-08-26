"""
ORB long (breakout) -- entry point for the realtime dispatch loop.

Thin wrapper around the shared ``ORBStrategy`` class in
``src.strategies.orb_shared.strategy_class``. The class owns every
phase (filters, reference, gate, detect, stop, fire); this module only
wires the long-direction instance -- direction, signal name, viz
state, and its OWN filters module (``.filters.filters``) -- and
re-exports its ``.run`` under a stable name (``orb_breakout``) that the
registry references directly. Filters are NOT shared with ORB short;
each direction has its own filter module.
"""

from __future__ import annotations

from src.strategies.orb_shared.strategy_class import ORBStrategy

from .filters import filters as long_filters
from .visualization import state as viz


# Signal name used in the alarm row + Telegram message when this fires.
ORB_LONG_SIGNAL_NAME: str = "ORB long breakout"


_impl = ORBStrategy(
    direction="long",
    signal_name=ORB_LONG_SIGNAL_NAME,
    viz=viz,
    filters=long_filters,
)


async def orb_breakout(bar, symbol: str) -> None:
    """Realtime entry hook -- delegates to the shared ORB orchestrator."""
    await _impl.run(bar, symbol)
