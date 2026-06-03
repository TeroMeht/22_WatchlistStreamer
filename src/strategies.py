from src.alarms.alarm_logics import *
from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order, detect_stoplevel
from src.database.db_functions import get_last_rows, get_session_rows
import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.exit_strategies import *
from src.core.config import settings
from src.core.strategy_registry import History, registry

logger = logging.getLogger(__name__)


# =============================================================================
# Strategy implementations
# =============================================================================
# These keep their original signatures so they stay easy to read/test. The
# registry adapters at the bottom of this file translate the uniform
# (candle, history) call into whatever each one expects.
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


async def vwapcontinuation_strategies(past_dataSet: pd.DataFrame, candle: CandleRow):

    logger.info("Running VWAP Continuation strategies for symbol: %s", candle.symbol)

    df_all = past_dataSet
    # Guard: ``history.session`` is empty (no columns) when get_session_rows
    # returned zero rows — happens when the first live candle arrives before
    # any in-session history has accumulated for this symbol (fresh DB after
    # ``delete_all_tables_db_async``, ticker added mid-session, running before
    # SESSION_START, etc). Skip cleanly instead of KeyError'ing on "Relatr".
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
                                        signal_name="VWAP continuation setup")


async def downside_extension(candle: CandleRow):

    logger.info(f"Capitulation alarm: {candle.symbol} with Relatr: {candle.relatR:.3f}")

    await generate_signal_alarm(candle=candle,
                                signal_name=f"Capitulation alarm")


async def upside_extension(candle: CandleRow):
    logger.info(f"Euforic extension detected for symbol: {candle.symbol} with Relatr: {candle.relatR:.3f}")

    await generate_signal_alarm(candle=candle,
                                signal_name=f"Euforic alarm")


# =============================================================================
# Registry wiring
# =============================================================================
# Each call below registers one strategy with:
#   - a guard          : cheap predicate on the candle
#   - a runner         : async fn taking (candle, History)
#   - needs_rows       : last-N rows pulled from the DB. Dispatcher fetches
#                        max(needs_rows) across active strategies ONCE per
#                        candle and exposes it as history.last_rows.
#   - needs_session    : if True, dispatcher fetches every row of today's
#                        session (Date == today AND Time >= SESSION_START)
#                        ONCE per candle and exposes it as history.session.
#                        Use this for strategies that need full intraday
#                        context (e.g. VWAP continuation).
#
# To turn a strategy on/off, edit strategies.toml — no code changes needed.
# To add a new strategy: write the coroutine, register it here, add a block in
# strategies.toml.
# =============================================================================


async def _reversal_long_runner(candle: CandleRow, history: History):
    await reversal_strategy(history.last_rows.tail(8), candle)


async def _reversal_short_runner(candle: CandleRow, history: History):
    await reversal_short_strategy(history.last_rows.tail(8), candle)


async def _vwap_continuation_runner(candle: CandleRow, history: History):
    # Needs everything since session open (set via needs_session=True below).
    await vwapcontinuation_strategies(history.session, candle)


async def _vwap_exit_runner(candle: CandleRow, history: History):
    await vwap_exit_strategy(candle)


async def _downside_extension_runner(candle: CandleRow, history: History):
    await downside_extension(candle)


async def _upside_extension_runner(candle: CandleRow, history: History):
    await upside_extension(candle)


async def _relatr_up_exit_runner(candle: CandleRow, history: History):
    await relatr_up_exit_strategy(candle)


async def _relatr_down_exit_runner(candle: CandleRow, history: History):
    await relatr_down_exit_strategy(candle)


async def _endofday_exit_runner(candle: CandleRow, history: History):
    await endofday_exit_strategy(candle)


# --- Entry strategies --------------------------------------------------------
registry.register(
    "reversal_long",
    guard=lambda c: c.relatR > 0,
    runner=_reversal_long_runner,
    needs_rows=8,
)

registry.register(
    "reversal_short",
    guard=lambda c: c.relatR < 0,
    runner=_reversal_short_runner,
    needs_rows=8,
)

registry.register(
    "vwap_continuation",
    guard=lambda c: is_vwap_close(c, settings.VWAP_DISTANCE),
    runner=_vwap_continuation_runner,
    needs_session=True,  # needs every row since SESSION_START, not a fixed N
)

registry.register(
    "downside_extension",
    guard=lambda c: c.relatR >= settings.EXTREME_EXTENSION_THRESHOLD and c.rvol >= 1.5,
    runner=_downside_extension_runner,
)

registry.register(
    "upside_extension",
    guard=lambda c: c.relatR <= -settings.EXTREME_EXTENSION_THRESHOLD and c.rvol >= 1.5,
    runner=_upside_extension_runner,
)

# --- Exit strategies ---------------------------------------------------------
registry.register(
    "vwap_exit",
    guard=lambda c: is_vwap_close(c, settings.VWAP_DISTANCE),
    runner=_vwap_exit_runner,
)

registry.register(
    "relatr_up_exit",
    guard=lambda c: c.relatR <= settings.EUFORIC_THRESHOLD,
    runner=_relatr_up_exit_runner,
)

registry.register(
    "relatr_down_exit",
    guard=lambda c: c.relatR >= settings.CAPITULATION_THRESHOLD,
    runner=_relatr_down_exit_runner,
)

registry.register(
    "endofday_exit",
    guard=lambda c: c.time >= settings.ENDOFDAY,
    runner=_endofday_exit_runner,
)


# Load toggles once at import time. strategies.toml lives at the project root
# (two levels up from this file: src/strategies.py -> project root).
_TOGGLES_PATH = Path(__file__).resolve().parent.parent / "strategies.toml"
registry.load_toggles(_TOGGLES_PATH)


# =============================================================================
# Per-ticker entry strategy selection
# =============================================================================
# Tickers + the entry strategies the user picked for each one are stored in the
# `watchlist` / `watchlist_strategies` tables and edited from the 26_ReactFastApp
# UI. The streamer loads that mapping once at startup (see run_streamer) and
# pushes it here via set_watchlist_strategies(). run_strategies() then filters
# the active entry strategies by what the user picked for *this* symbol.
#
# Exit strategies are intentionally NOT filtered — they remain globally enabled
# via strategies.toml.
#
# Restart-the-streamer to pick up changes (matches the simple refresh model
# chosen in the design discussion).
# =============================================================================

# Names of entry strategies (must match registry.register(...) names above).
# Used to split "entry" from "exit" when filtering by per-ticker selection.
_ENTRY_STRATEGY_NAMES: frozenset[str] = frozenset({
    "reversal_long",
    "reversal_short",
    "vwap_continuation",
    "downside_extension",
    "upside_extension",
})

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
    # Normalize: uppercase symbols, set() of names. Don't mutate caller's dict.
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
    """Run every enabled strategy whose guard fires for this candle.

    Each kind of history is fetched at most ONCE per candle:
      - last_rows  : depth = max(needs_rows) across active strategies
      - session    : full session (Date == today AND Time >= SESSION_START)
                     if any active strategy set needs_session=True
    The two fetches run in parallel when both are required.
    """
    active = registry.active_for(candle)
    if not active:
        return

    # --- Filter entry strategies by the user's per-ticker selection ---------
    # _watchlist_strategies is populated at startup from the
    # `watchlist_strategies` table. Exit strategies are NOT filtered here —
    # they remain globally enabled via strategies.toml so things like
    # vwap_exit / endofday_exit keep firing for every symbol that has a
    # position.
    allowed_for_symbol = _watchlist_strategies.get(candle.symbol.upper(), set())
    active = [
        s for s in active
        if s.name not in _ENTRY_STRATEGY_NAMES or s.name in allowed_for_symbol
    ]
    if not active:
        return

    table_name = f"{candle.symbol.lower()}_livestream"
    max_rows = max((s.needs_rows for s in active), default=0)
    need_session = any(s.needs_session for s in active)

    # Fetch whichever history kinds are needed, in parallel.
    history = History()
    fetches: list[tuple[str, Any]] = []
    if max_rows > 0:
        fetches.append(("last_rows", get_last_rows(table_name=table_name, num_rows=max_rows)))
    if need_session:
        fetches.append(("session", get_session_rows(
            table_name=table_name,
            day=candle.date,
            since_time=settings.SESSION_START,
        )))

    if fetches:
        keys = [k for k, _ in fetches]
        results = await asyncio.gather(*(c for _, c in fetches))
        for key, result in zip(keys, results):
            setattr(history, key, result)

    await asyncio.gather(*(s.runner(candle, history) for s in active))
