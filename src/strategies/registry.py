"""
Strategy registry.

The single source of truth mapping strategy NAMES (as stored in the
watchlist / exit_requests tables) to the async CALLABLES that implement
them. Adding a new strategy = one import + one entry in the appropriate
dict below. No other file needs to change (the dispatcher iterates these
dicts).

Three registries, keyed by cadence + role:

* ``ENTRY_STRATEGIES``          -- 2-min candle-driven entries.
                                    Callable signature: ``fn(candle) -> coro``.
* ``EXIT_STRATEGIES``           -- 2-min candle-driven exits.
                                    Callable signature: ``fn(candle) -> coro``.
* ``REALTIME_ENTRY_STRATEGIES`` -- 5-sec bar-driven entries.
                                    Callable signature: ``fn(bar, symbol) -> coro``.
"""

from __future__ import annotations

from src.strategies.entry_strategies import *
from src.strategies.exit_strategies import *
from src.strategies.orb_strategy_long.strategy import orb_breakout_long


# Candle-driven (2-min) entries. Callable: fn(candle) -> coroutine.
ENTRY_STRATEGIES: dict = {
    "reversal_long":            reversal_strategy,
    "reversal_short":           reversal_short_strategy,
    "vwap_continuation_long":   vwapcontinuation_long_strategy,
    "vwap_continuation_short":  vwapcontinuation_short_strategy,
}

# Candle-driven (2-min) exits. Callable: fn(candle) -> coroutine.
EXIT_STRATEGIES: dict = {
    "vwap_exit":           vwap_exit_strategy,
    "momentum_long_exit":  momentum_long_exit,
    "momentum_short_exit": momentum_short_exit,
    "trim_into_strength":  trim_into_strength,
    "trim_into_weakness":  trim_into_weakness,
    "endofday_exit":       endofday_exit_strategy,
}

# Realtime (5-sec) entries. Callable: fn(bar, symbol) -> coroutine.
REALTIME_ENTRY_STRATEGIES: dict = {
    "orb_breakout_long": orb_breakout_long,
}
