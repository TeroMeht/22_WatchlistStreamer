from src.alarms.alarm_logics import *
from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order, detect_stoplevel
from src.database.db_functions import get_last_rows, get_session_rows
from src.database.exit_requests import load_armed_exit_strategies
import asyncio
import logging
import time as _time
from typing import Dict, Set

import pandas as pd

from src.exit_strategies import *
from src.core.config import settings

logger = logging.getLogger(__name__)


# =============================================================================
# Armed-exits cache (read from the shared exit_requests table)
# =============================================================================
# Exit strategies fire only when the (symbol, strategy) row is armed in the
# DB — same model as entry strategies being filtered by watchlist_strategies.
# Users arm and disarm exits from the 26_ReactFastApp UI throughout the
# session, so we re-read the table on a short TTL instead of snapshotting at
# startup. The query is small (single small table, no joins) so the load is
# negligible even at one refresh per candle.

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


# =============================================================================
# Strategy implementations
# =============================================================================


async def reversal_strategy(last_8_rows: pd.DataFrame, candle: CandleRow):

    logger.info("Running Reversal Long strategy for symbol: %s | RelATR: %.4f", candle.symbol, candle.relatR)

    # Check for capitulation using the DataFrame
    if detect_capitulation(last_8_rows, threshold=settings.CAPITULATION_THRESHOLD):
        logger.debug("Capitulation detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_up(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(last_8_rows, direction="long")

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover up"
            )

            await generate_entry_order(
                candle=candle,
                stop_level=stop_level
            )


async def reversal_short_strategy(last_8_rows: pd.DataFrame, candle: CandleRow):

    logger.info("Running Reversal Short strategy for symbol: %s | RelATR: %.4f", candle.symbol, candle.relatR)

    # Check for euforia using the DataFrame
    if detect_euforia(last_8_rows, threshold=settings.CAPITULATION_THRESHOLD):
        logger.debug("Euforia detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_down(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover down detected, generating alarm...")

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(last_8_rows, direction="short")
            logging.info(f"{candle.symbol}: Stop level set at {stop_level}")

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover down"
            )
            await generate_entry_order(
                candle=candle,
                stop_level=stop_level
            )


async def vwapcontinuation_short_strategy(past_dataSet: pd.DataFrame, candle: CandleRow):

    logger.info("Running VWAP Continuation short strategy for symbol: %s", candle.symbol)

    df_all = past_dataSet

    if df_all is None or df_all.empty or "Relatr" not in df_all.columns:
        logger.debug(
            "VWAP continuation: no session history for %s yet — skipping.",
            candle.symbol,
        )
        return

    # Tarvii tarkastaa että onhan hinta ollut viimeaikoina VWAP yläpuolella
    last_5 = df_all.tail(5)
    avg_relatr = last_5["Relatr"].mean()  # lasketaan viimeisimpien 5 candlejen relatr keskiarvo. Tämän etumerkin perusteella päätellään missä hinta on ollut


    if avg_relatr > 0: # to the downside
        # Log the average Relatr for debugging/visibility
        logger.info(
            "Price has been below VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
            candle.symbol,
            avg_relatr,
        )
        # Detect earlier capitulation
        if detect_capitulation(df_all, threshold=settings.CAPITULATION_THRESHOLD):
            logger.info(f"Capitulation detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

            await generate_signal_alarm(candle=candle,
                                        signal_name="VWAP continuation short setup")


async def vwapcontinuation_long_strategy(past_dataSet: pd.DataFrame, candle: CandleRow):

    logger.info("Running VWAP Continuation long strategy for symbol: %s", candle.symbol)

    df_all = past_dataSet
    if df_all is None or df_all.empty or "Relatr" not in df_all.columns:
        logger.debug(
            "VWAP continuation: no session history for %s yet — skipping.",
            candle.symbol,
        )
        return

    # Tarvii tarkastaa että onhan hinta ollut viimeaikoina VWAP yläpuolella
    last_5 = df_all.tail(5)
    avg_relatr = last_5["Relatr"].mean()  # lasketaan viimeisimpien 5 candlejen relatr keskiarvo. Tämän etumerkin perusteella päätellään missä hinta on ollut


    # Tarkoituksena käyttää samaa kannasta haettua dataa jottei sitä tarvi kysellä uudelleen
    if avg_relatr < 0:  # jos tää on ollut pienempi kuin 0 niin on oltu VWAP yläpuolella
        # Log the average Relatr for debugging/visibility
        logger.info(
            "Price has been above VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
            candle.symbol,
            avg_relatr,
        )
        # Detect euforia
        if detect_euforia(df_all, threshold=settings.CAPITULATION_THRESHOLD):
            logger.info(f"Euforia detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

            await generate_signal_alarm(candle=candle,
                                        signal_name="VWAP continuation long setup")


# =============================================================================
# Per-ticker entry strategy selection
# =============================================================================
# Tickers + the entry strategies the user picked for each one are stored in the
# `watchlist` / `watchlist_strategies` tables and edited from the 26_ReactFastApp
# UI. The streamer loads that mapping once at startup (see run_streamer) and
# pushes it here via set_watchlist_strategies(). run_strategies() then filters
# the active entry strategies by what the user picked for *this* symbol.
#
# Exit strategies are intentionally NOT filtered — they run for every symbol
# that produces a candle.
#
# Restart the streamer to pick up changes (matches the simple refresh model
# chosen in the design discussion).
# =============================================================================



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
# Dispatch
# =============================================================================


async def run_strategies(candle: CandleRow):
    """Run every applicable strategy for this candle.

    Entry strategies are filtered by the user's per-ticker selection
    (_watchlist_strategies). Exit strategies are filtered by which
    (symbol, strategy) rows are currently armed in the shared
    exit_requests table — refreshed on a short TTL so UI edits propagate
    within seconds. Each kind of candle history is fetched at most once
    per candle, in parallel when both are needed.
    """
    allowed = _watchlist_strategies.get(candle.symbol.upper(), set())
    armed_exits = await _get_armed_exits_for(candle.symbol)
    vwap_close = is_vwap_close(candle, settings.VWAP_DISTANCE)

    # Entry guards (also gated by per-ticker selection).
    run_reversal_long = candle.relatR > 0 and "reversal_long" in allowed
    run_reversal_short = candle.relatR < 0 and "reversal_short" in allowed
    run_vwap_cont_long = vwap_close and "vwap_continuation_long" in allowed
    run_vwap_cont_short = vwap_close and "vwap_continuation_short" in allowed

    # Exit guards — must be *both* in the right market condition AND armed
    # for this symbol in exit_requests. No armed row = no streamer fire.
    run_vwap_exit = vwap_close and "vwap_exit" in armed_exits
    run_momentum_long_exit = "momentum_long_exit" in armed_exits
    run_momentum_short_exit = "momentum_short_exit" in armed_exits
    run_endofday_exit = (
        candle.time >= settings.ENDOFDAY and "endofday_exit" in armed_exits
    )

    # Fetch only the history kinds that are actually needed, in parallel.
    # Momentum exits also need the last 8 rows (euforia/capitulation + EMA9
    # crossover check), so they extend need_last_rows alongside the reversal
    # entries.
    table_name = f"{candle.symbol.lower()}_livestream"
    fetches = {}
    if (run_reversal_long or run_reversal_short
            or run_momentum_long_exit or run_momentum_short_exit):
        fetches["last_rows"] = get_last_rows(table_name=table_name, num_rows=8)
    if run_vwap_cont_long or run_vwap_cont_short:
        fetches["session"] = get_session_rows(
            table_name=table_name,
            day=candle.date,
            since_time=settings.SESSION_START,
        )
    results = dict(zip(fetches, await asyncio.gather(*fetches.values())))
    last_rows = results.get("last_rows")
    session = results.get("session")

    coros = []
    if run_reversal_long:
        coros.append(reversal_strategy(last_rows.tail(8), candle))
    if run_reversal_short:
        coros.append(reversal_short_strategy(last_rows.tail(8), candle))
    if run_vwap_cont_long:
        coros.append(vwapcontinuation_long_strategy(session, candle))
    if run_vwap_cont_short:
        coros.append(vwapcontinuation_short_strategy(session, candle))
        
    if run_vwap_exit:
        coros.append(vwap_exit_strategy(candle))
    if run_momentum_long_exit:
        coros.append(momentum_long_exit(last_rows.tail(8), candle))
    if run_momentum_short_exit:
        coros.append(momentum_short_exit(last_rows.tail(8), candle))
    if run_endofday_exit:
        coros.append(endofday_exit_strategy(candle))

    await asyncio.gather(*coros)
