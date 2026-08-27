"""
reversal_long -- entry point for the 2-min candle dispatch loop.

Candle-driven, NOT realtime: fires from ``run_strategies(candle)`` on
every finalized 2-min ``CandleRow``, same cadence as
``vwap_continuation_long``. Delegates the full flow to the shared
``ReversalStrategy`` class; this module only wires the long-direction
instance -- direction, signal name, viz state, and its OWN filters
module -- and re-exports its ``.run`` under a stable name
(``reversal_long``) that the registry references directly. Filters are
NOT shared with reversal_short; each direction has its own filter
module.
"""

from __future__ import annotations

from src.helpers.handle_candles import CandleRow
from src.strategies.reversal_shared.strategy_class import ReversalStrategy

from .filters import filters as long_filters
from .visualization import state as viz


# Signal name used in the alarm row + Telegram message when this fires.
REVERSAL_LONG_SIGNAL_NAME: str = "reversal_long"


_impl = ReversalStrategy(
    direction="long",
    signal_name=REVERSAL_LONG_SIGNAL_NAME,
    viz=viz,
    filters=long_filters,
)


async def reversal_long(candle: CandleRow) -> None:
    """2-min candle entry hook -- delegates to the shared reversal orchestrator."""
    await _impl.run(candle)
