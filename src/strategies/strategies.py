"""
Top-level strategy dispatcher.

Two entry points:

* ``run_strategies(candle)`` -- called once per 2-min ``CandleRow`` from
  ``finalize_candle``. Fires every registered entry strategy for which
  the symbol is armed on the watchlist AND every registered exit
  strategy for which the symbol has an armed exit request. Concurrent.

* ``run_realtime_strategies(bar, symbol)`` -- called on every 5-sec bar
  from ``process_bar``. Fires the registered realtime entry strategies
  (e.g. ORB breakout / breakdown) which need sub-candle responsiveness
  because waiting for the next 2-min close would be too slow.

Registries are plain lists of functions. A strategy is armed for a
symbol when its ``fn.__name__`` appears in the watchlist / exit-request
set for that symbol. Adding a strategy = one line in ``registry.py``,
not here.

Re-exports ``set_watchlist_strategies`` / ``get_watchlist_strategies``
from ``dispatcher_state`` so existing callers of
``from src.strategies import *`` still find them.
"""

from __future__ import annotations

import asyncio
import logging

from src.helpers.handle_candles import CandleRow
from src.strategies.registry import (
    ENTRY_STRATEGIES,
    EXIT_STRATEGIES,
    REALTIME_ENTRY_STRATEGIES,
)
from src.strategies.dispatcher_state import (  # re-exported for back-compat
    get_armed_exits_for,
    get_watchlist_strategies,
    get_watchlist_strategies_for,
    set_watchlist_strategies,
)

logger = logging.getLogger(__name__)


async def run_strategies(candle: CandleRow) -> None:
    """
    For each incoming 2-min candle: fire every armed entry + exit
    strategy concurrently. Match strategies to the watchlist / exit
    requests by ``fn.__name__``.
    """
    allowed_entries = get_watchlist_strategies_for(candle.symbol)
    armed_exits = await get_armed_exits_for(candle.symbol)

    coros = [
        *(fn(candle) for fn in ENTRY_STRATEGIES if fn.__name__ in allowed_entries),
        *(fn(candle) for fn in EXIT_STRATEGIES  if fn.__name__ in armed_exits),
    ]
    if coros:
        await asyncio.gather(*coros)


async def run_realtime_strategies(bar, symbol: str) -> None:
    """
    Run per-bar entry strategies for every incoming 5-sec ``bar``.

    Called from ``process_bar`` on every 5-sec tick, *in addition to*
    the 2-min ``run_strategies`` fired from ``finalize_candle``. Kept
    lean -- strategies here should return immediately when their gate
    is closed or their reference data isn't ready yet.
    """
    allowed_entries = get_watchlist_strategies_for(symbol)
    coros = [
        fn(bar, symbol)
        for fn in REALTIME_ENTRY_STRATEGIES
        if fn.__name__ in allowed_entries
    ]
    if coros:
        await asyncio.gather(*coros)
