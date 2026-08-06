"""
Cross-strategy in-memory state used by the dispatcher.

Two independent caches live here so ``strategies.py`` stays focused on
dispatch shape only:

* Watchlist strategies -- ``{SYMBOL_UPPER: {strategy_name, ...}}``.
  Seeded once at streamer startup from the DB via
  ``set_watchlist_strategies`` and read on every 2-min candle / 5-sec
  bar. UI edits take effect on the next streamer restart (agreed
  refresh model).

* Armed exit strategies -- ``{SYMBOL_UPPER: {strategy_name, ...}}``.
  Refreshed from the DB every ``_ARMED_EXITS_TTL_SECONDS`` so
  newly-armed / disarmed rows take effect within a few seconds without
  hammering the DB on every candle.

Named ``dispatcher_state`` (not just ``state``) so it doesn't collide
with the per-strategy ``state.py`` files under ``orb_long/``,
``reversal_long/``, etc. -- those hold per-strategy session state
(fire latches, yesterday-level caches), which is a different concern.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time
from typing import Dict, Set

from src.database.exit_requests import load_armed_exit_strategies

logger = logging.getLogger(__name__)


# =============================================================================
# Watchlist strategies (per-ticker entry strategy selection)
# =============================================================================

# Mapping {SYMBOL_UPPER: {strategy_name, ...}}. Empty by default -- set_*
# is called from prepare_watchlist() after loading from the DB.
_watchlist_strategies: Dict[str, Set[str]] = {}


def set_watchlist_strategies(mapping: Dict[str, Set[str]]) -> None:
    """
    Cache the per-ticker entry strategy selection in memory.

    Called once at streamer startup. Subsequent edits in the UI take effect
    on the next streamer restart, which is the agreed refresh model.
    """
    global _watchlist_strategies
    _watchlist_strategies = {
        (sym or "").upper(): set(strats or ())
        for sym, strats in mapping.items()
    }
    logger.info(
        "Watchlist strategy cache loaded: %d symbols, %d total bindings",
        len(_watchlist_strategies),
        sum(len(v) for v in _watchlist_strategies.values()),
    )


def get_watchlist_strategies() -> Dict[str, Set[str]]:
    """Return a copy of the cached mapping (debug/inspection)."""
    return {k: set(v) for k, v in _watchlist_strategies.items()}


def get_watchlist_strategies_for(symbol: str) -> Set[str]:
    """Return the set of entry strategies armed for ``symbol`` (empty if none)."""
    return _watchlist_strategies.get(symbol.upper(), set())


# =============================================================================
# Armed exit strategies (per-ticker exit strategy selection, TTL-refreshed)
# =============================================================================

_armed_exits_cache: Dict[str, Set[str]] = {}
_armed_exits_cache_at: float = 0.0
_ARMED_EXITS_TTL_SECONDS: float = 5.0
_armed_exits_lock = asyncio.Lock()


async def get_armed_exits_for(symbol: str) -> Set[str]:
    """
    Return the set of exit-strategy names currently armed for ``symbol``.
    Refreshes the in-memory cache every ``_ARMED_EXITS_TTL_SECONDS`` so
    newly armed / disarmed rows take effect within a few seconds.
    """
    global _armed_exits_cache, _armed_exits_cache_at
    now = _time.monotonic()
    if now - _armed_exits_cache_at > _ARMED_EXITS_TTL_SECONDS:
        async with _armed_exits_lock:
            # Double-check inside the lock so concurrent candles for
            # different symbols don't all reload simultaneously.
            if _time.monotonic() - _armed_exits_cache_at > _ARMED_EXITS_TTL_SECONDS:
                _armed_exits_cache = await load_armed_exit_strategies()
                _armed_exits_cache_at = _time.monotonic()
                logger.debug(
                    "Refreshed armed exits cache: %d symbols, %d bindings",
                    len(_armed_exits_cache),
                    sum(len(v) for v in _armed_exits_cache.values()),
                )
    return _armed_exits_cache.get(symbol.upper(), set())
