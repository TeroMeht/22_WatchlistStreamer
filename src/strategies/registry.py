"""
Strategy registry.

The single source of truth for the strategy CALLABLES the dispatcher
knows about. Adding a strategy = one import + one append; no other
file needs to change (the dispatcher iterates these lists).

Each list holds bare function objects. The dispatcher identifies a
strategy by ``fn.__name__``, which must match the strategy name stored
in the watchlist / exit_requests tables. No more ``"name": fn`` dict --
that mapping was hand-maintained on every add, and getting it wrong
silently disabled a strategy.

Three registries, keyed by cadence + role:

* ``ENTRY_STRATEGIES``          -- 2-min candle-driven entries.
                                    Callable signature: ``fn(candle) -> coro``.
* ``EXIT_STRATEGIES``           -- 2-min candle-driven exits.
                                    Callable signature: ``fn(candle) -> coro``.
* ``REALTIME_ENTRY_STRATEGIES`` -- 5-sec bar-driven entries.
                                    Callable signature: ``fn(bar, symbol) -> coro``.
"""

from __future__ import annotations

from src.strategies.entry_strategies import (
    reversal_long,
)
from src.strategies.exit_strategies import (
    endofday_exit,
    momentum_long_exit,
    momentum_short_exit,
    trim_into_strength,
    trim_into_weakness,
    vwap_exit,
)
from src.strategies.orb_long.strategy import orb_breakout
from src.strategies.orb_short.strategy import orb_breakdown
from src.strategies.vwap_continuation_long.strategy import vwap_continuation_long


# Candle-driven (2-min) entries. Callable: fn(candle) -> coroutine.
ENTRY_STRATEGIES: list = [
    reversal_long,
    vwap_continuation_long,
]

# Candle-driven (2-min) exits. Callable: fn(candle) -> coroutine.
EXIT_STRATEGIES: list = [
    vwap_exit,
    momentum_long_exit,
    momentum_short_exit,
    trim_into_strength,
    trim_into_weakness,
    endofday_exit,
]

# Realtime (5-sec) entries. Callable: fn(bar, symbol) -> coroutine.
REALTIME_ENTRY_STRATEGIES: list = [
    orb_breakout,
    orb_breakdown,
]
