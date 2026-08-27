"""
reversal_short -- entry point for the 2-min candle dispatch loop.

Mirror of ``reversal_long`` with ``direction="short"``: fires when the
last 2 candles show an EMA9 crossover DOWN and any of the last 8
candles has ``Relatr <= EUFORIC_THRESHOLD``. Stop is anchored ABOVE
the recent range via the shared ``detect_stoplevel`` in short mode.
Filters are NOT shared with reversal_long -- this instance wires its
own ``.filters.filters`` module.
"""

from __future__ import annotations

from src.helpers.handle_candles import CandleRow
from src.strategies.reversal_shared.strategy_class import ReversalStrategy

from .filters import filters as short_filters
from .visualization import state as viz


# Signal name used in the alarm row + Telegram message when this fires.
REVERSAL_SHORT_SIGNAL_NAME: str = "reversal_short"


_impl = ReversalStrategy(
    direction="short",
    signal_name=REVERSAL_SHORT_SIGNAL_NAME,
    viz=viz,
    filters=short_filters,
)


async def reversal_short(candle: CandleRow) -> None:
    """2-min candle entry hook -- delegates to the shared reversal orchestrator."""
    await _impl.run(candle)
