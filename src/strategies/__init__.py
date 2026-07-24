from src.alarms.alarm_logics import *
from src.database.db_functions import get_last_rows, get_session_rows
from src.database.exit_requests import load_armed_exit_strategies
import asyncio
import logging
import time as _time
from typing import Dict, Set



from src.exit_strategies import *
from src.entry_strategies import *
from src.strategies.orb_strategy.strategy import orb_breakout_long
from src.core.config import settings

logger = logging.getLogger(__name__)




_armed_exits_cache: Dict[str, Set[str]] = {}
_armed_exits_cache_at: float = 0.0
_ARMED_EXITS_TTL_SECONDS: float = 5.0
_armed_exits_lock = asyncio.Lock()


async def _get_armed_exits_for(symbol: str) -> Set[str]:
    """
    Return the set of strategy names currently armed for `symbol`.
    Refreshes the in-memory cache every _ARMED_EXITS_TTL_SECONDS so newly
    armed / disarmed rows take effect within a few seconds.
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






# Mapping {SYMBOL_UPPER: {strategy_name, ...}}. Empty by default — set_*
# is called from run_streamer() after loading from the DB.
_watchlist_strategies: dict[str, set[str]] = {}


def set_watchlist_strategies(mapping: dict[str, set[str]]) -> None:
    """
    Cache the per-ticker entry strategy selection in memory.

    Called once at streamer startup. Subsequent edits in the UI take effect on
    the next streamer restart, which is the agreed refresh model.
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


def get_watchlist_strategies() -> dict[str, set[str]]:
    """Return a copy of the cached mapping (debug/inspection)."""
    return {k: set(v) for k, v in _watchlist_strategies.items()}


# =============================================================================
# Entry dispatch
# =============================================================================


def _gate_entries(allowed: Set[str]) -> dict:
    """Which entry strategies should fire for this candle."""
    return {
        "reversal_long":          "reversal_long" in allowed,
        "reversal_short":         "reversal_short" in allowed,
        "vwap_continuation_long": "vwap_continuation_long" in allowed,
        "vwap_continuation_short": "vwap_continuation_short" in allowed,
    }


def _entry_coros(candle: CandleRow, gates: dict) -> list:
    """Build the entry-strategy coroutines that passed their gates."""
    coros = []
    if gates["reversal_long"]:
        coros.append(reversal_strategy(candle))
    if gates["reversal_short"]:
        coros.append(reversal_short_strategy(candle))
    if gates["vwap_continuation_long"]:
        coros.append(vwapcontinuation_long_strategy(candle))
    if gates["vwap_continuation_short"]:
        coros.append(vwapcontinuation_short_strategy(candle))
    return coros


# =============================================================================
# Exit dispatch
# =============================================================================


def _gate_exits(armed: Set[str]) -> dict:
    """Which exit strategies should fire for this candle."""
    return {
        "vwap_exit":            "vwap_exit" in armed,
        "trim_into_strength":   "trim_into_strength" in armed,
        "trim_into_weakness":   "trim_into_weakness" in armed,
        "momentum_long_exit":   "momentum_long_exit" in armed,
        "momentum_short_exit":  "momentum_short_exit" in armed,
        "endofday_exit":        "endofday_exit" in armed,
    }


def _exit_coros(candle: CandleRow, gates: dict) -> list:
    """Build the exit-strategy coroutines that passed their gates."""
    coros = []
    if gates["vwap_exit"]:
        coros.append(vwap_exit_strategy(candle))
    if gates["momentum_long_exit"]:
        coros.append(momentum_long_exit(candle))
    if gates["momentum_short_exit"]:
        coros.append(momentum_short_exit(candle))
    if gates["trim_into_strength"]:
        coros.append(trim_into_strength(candle))
    if gates["trim_into_weakness"]:
        coros.append(trim_into_weakness(candle))
    if gates["endofday_exit"]:
        coros.append(endofday_exit_strategy(candle))
    return coros




# =============================================================================
# Top-level dispatcher
# =============================================================================


async def run_strategies(candle: CandleRow):
    """
    For each incoming candle:
      1. Decide which entries and exits are eligible (gates).
      2. Fetch the shared history they need.
      3. Fire entry + exit coroutines concurrently.
    """
    allowed = _watchlist_strategies.get(candle.symbol.upper(), set())
    armed_exits = await _get_armed_exits_for(candle.symbol)

    entry_gates = _gate_entries(allowed)
    exit_gates = _gate_exits(armed_exits)


    coros = [
        *_entry_coros(candle, entry_gates),
        *_exit_coros(candle, exit_gates),
    ]
    await asyncio.gather(*coros)


# =============================================================================
# Realtime (5-sec) entry dispatch
# =============================================================================
#
# Some entries -- e.g. ORB breakout -- need to fire the instant price crosses
# a level, so waiting for the next 2-min candle to finalize is too slow. They
# run on every 5-sec bar in their own dispatcher, gated by the same
# per-ticker watchlist mapping used above.


def _gate_realtime_entries(allowed: Set[str]) -> dict:
    """Which realtime (5-sec) entry strategies should fire for this bar."""
    return {
        "orb_breakout_long": "orb_breakout_long" in allowed,
    }


def _realtime_entry_coros(bar, symbol: str, gates: dict) -> list:
    """Build the realtime entry-strategy coroutines that passed their gates."""
    coros = []
    if gates["orb_breakout_long"]:
        coros.append(orb_breakout_long(bar, symbol))
    return coros


async def run_realtime_strategies(bar, symbol: str) -> None:
    """
    Run per-bar entry strategies for every incoming 5-sec ``bar``.

    Called from ``process_bar`` on every 5-sec tick, *in addition to* the
    2-min ``run_strategies`` fired from ``finalize_candle``. Kept lean --
    strategies here should return immediately when their gate is closed or
    their reference data isn't ready yet.
    """
    allowed = _watchlist_strategies.get(symbol.upper(), set())
    entry_gates = _gate_realtime_entries(allowed)
    coros = _realtime_entry_coros(bar, symbol, entry_gates)
    if not coros:
        return
    await asyncio.gather(*coros)
