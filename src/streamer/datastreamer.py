import asyncio
import logging
import os
from typing import Optional

from ib_async import IB

from src.alarms.send_postrequest import send_streamer_status
from src.core.config import settings
from src.database.db_functions import (
    archive_livestream_tables,
    create_alarms_table,
    create_and_fill_avg_volume_tables_async,
    create_and_fill_table_async,
    create_orders_table,
    delete_all_tables_db_async,
)
from src.database.exit_requests import load_armed_exit_strategies
from src.database.watchlist import create_watchlist_tables, load_watchlist
from src.dependencies import init_db_pool
from src.helpers.handle_dataframes import (
    handle_Atr_intraday_dataset,
    handle_intraday_rvol_dataset,
)
from src.helpers.ibclient import (
    fetch_history_daily,
    fetch_intraday_volume_history,
    monitor_tickers,
)
from src.helpers.process_incoming_data import CandleStore
from src.strategies import warmup
from src.strategies.dispatcher_state import set_watchlist_strategies
from src.strategies.visualization.dashboard import start_dashboard
from src.streamer.datavalidation import validate_datasets, validate_tickers


# =============================================================================
# Phase 1 -- initialize app
# =============================================================================


async def initialize_app() -> IB:
    """
    Process-level bring-up. Returns the connected IB client the caller
    hands to ``data_pipe`` (and ``disconnect``s at shutdown):
        * Initialize the shared DB pool.
        * Open the IB gateway connection.
        * POST our PID to the backend so the UI dot flips green.
        * Bring up the strategy dashboard (localhost:8790).

    The DB pool set up here is process-global (see ``src.dependencies``);
    the caller is responsible for closing it via ``close_db_pool()`` at
    shutdown. Table setup is a separate concern -- see
    ``prepare_database`` -- and watchlist configuration is another --
    see ``prepare_watchlist``.
    """
    await init_db_pool()

    # Open the IB gateway connection. Fail fast here -- nothing downstream
    # works without a live IB session.
    ib = IB()
    await ib.connectAsync(
        settings.IB_HOST,
        settings.IB_PORT,
        clientId=settings.IB_CLIENT_ID,
    )
    logging.info(
        "IB connection established: %s:%d (clientId=%s)",
        settings.IB_HOST, settings.IB_PORT, settings.IB_CLIENT_ID,
    )

    # Notify the backend we're up (PID lets it watch for hard kills).
    await send_streamer_status(
        settings.STREAMER_START_ENDPOINT,
        label="start",
        payload={"pid": os.getpid()},
    )

    # Bring up the unified strategy dashboard (localhost:8790). One page
    # renders overlays from every strategy's viz state module. Non-fatal
    # if aiohttp isn't installed -- start_dashboard logs and returns None.
    await start_dashboard()

    return ib


# =============================================================================
# Phase 2 -- prepare watchlist (monitor set assembly)
# =============================================================================


async def prepare_watchlist() -> Optional[dict]:
    """
    Assemble the per-symbol monitor set the streamer will subscribe to and
    hand it to the strategy dispatcher.

    Merges two DB-backed configuration sources:
        * ``watchlist`` / ``watchlist_strategies`` -- source of truth for
          entry strategies per ticker, written by the UI.
        * ``exit_requests`` -- source of truth for armed exit strategies.
          Symbols armed for exit but NOT on the watchlist still need to
          receive candles (otherwise the exit would never fire), so they
          get pulled in with a synthetic empty entry-strategy set. The
          entry dispatcher skips them; exits still run normally.

    Returns the merged monitor set ``{SYMBOL: {strategy_name, ...}}`` or
    ``None`` if there's nothing to monitor (caller should return).
    Also pushes the mapping into the strategy dispatcher via
    ``set_watchlist_strategies`` so ``run_strategies()`` can filter entry
    strategies per ticker without another lookup.
    """
    watchlist = await load_watchlist()  # {SYMBOL: {strategy_name, ...}}

    armed_exits = await load_armed_exit_strategies()
    for symbol in armed_exits:
        watchlist.setdefault(symbol, set())

    if not watchlist:
        logging.warning(
            "Nothing to monitor: watchlist is empty AND no armed exit "
            "requests exist. Add tickers via the UI (POST /api/watchlist) "
            "or arm an exit plan; exiting."
        )
        return None

    from_watchlist = {s for s, strats in watchlist.items() if strats}
    from_exits_only = {s for s in armed_exits if not watchlist.get(s)}
    logging.info(
        "Monitor set: %d symbols (%d watchlist, %d exit-only)",
        len(watchlist), len(from_watchlist), len(from_exits_only),
    )

    set_watchlist_strategies(watchlist)

    return watchlist


# =============================================================================
# Phase 3 -- database preparation
# =============================================================================


async def prepare_database() -> None:
    await create_alarms_table()
    await create_orders_table()
    await create_watchlist_tables()
    await archive_livestream_tables()
    await delete_all_tables_db_async()


# =============================================================================
# Phase 4 -- history data fetch
# =============================================================================


async def _fetch_history_data(ib, tickers: list) -> tuple:
    """
    Pull each ticker's historical datasets from IB in parallel:
        * daily bars (useRTH=True, 14 days ending yesterday)
        * intraday volume history split into today's 2-min bars and the
          preceding 5 days' 2-min bars (returned as a pair by
          ``fetch_intraday_volume_history``)

    Returns a triple of same-length lists aligned with ``tickers``:
    ``(daily_data, today_intradaydata, past_intradaydata)``. Any entry
    may be ``None`` for a ticker whose fetch failed; the ticker
    validation phase filters those out.
    """
    daily_data, intraday_pairs = await asyncio.gather(
        asyncio.gather(*(fetch_history_daily(ib, t) for t in tickers)),
        asyncio.gather(*(fetch_intraday_volume_history(ib, t) for t in tickers)),
    )
    today_intradaydata = [pair[0] if pair else None for pair in intraday_pairs]
    past_intradaydata  = [pair[1] if pair else None for pair in intraday_pairs]
    return list(daily_data), today_intradaydata, past_intradaydata





# =============================================================================
# Phase 6 -- indicator calculations (Rvol / ATR / Relatr) + persist
# =============================================================================


async def _calculate_indicators(
    daily_data: list,
    today_intradaydata: list,
    past_intradaydata: list,
) -> tuple:
    """
    Enrich the raw history with the derived columns strategies need:
        * ``handle_intraday_rvol_dataset`` -- compute Rvol from today's
          2-min bars against the 5-day intraday-volume model.
        * ``handle_Atr_intraday_dataset`` -- compute ATR from daily bars,
          join it in as ``Relatr`` on the intraday frame, and expose the
          latest per-ticker ATR for use as the stop-distance seed.

    Then persist:
        * average-volume tables (one per ticker) written from the 5-day
          intraday-volume frames -- read by the live path to compute Rvol
          per finalized 2-min candle.
        * per-ticker ``*_livestream`` tables seeded with the enriched
          intraday history -- the base rows every strategy reads from.

    Returns ``(relatr_datasets, last_atr_dict)``. ``relatr_datasets`` is a
    dict ``{SYMBOL: DataFrame}`` of the enriched intraday history and is
    the input for the caller's in-memory state seeding.
    """
    rvol_dataset = handle_intraday_rvol_dataset(today_intradaydata, past_intradaydata)
    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(rvol_dataset, daily_data)

    await asyncio.gather(
        create_and_fill_avg_volume_tables_async(past_intradaydata),
        *(create_and_fill_table_async(df) for df in relatr_datasets.values()),
    )

    return relatr_datasets, last_atr_dict


# =============================================================================
# Phase 7 -- live data streamer
# =============================================================================


async def data_streamer(ib, valid_tickers: list, last_atr_dict: dict) -> None:
    """
    Subscribe to the live 5-sec bar stream for every valid ticker and
    dispatch each incoming bar through the strategy pipeline (via
    ``monitor_tickers``). Runs until the process is stopped; each
    ticker's subscription is one gathered coroutine, so cancellation of
    one doesn't cancel the others until the outer task is cancelled.

    ``last_atr_dict`` seeds each ticker's per-tick ATR helper --
    ``monitor_tickers`` refreshes it as new 2-min candles finalize.
    """
    logging.info("Starting live monitoring...")
    candle_store = CandleStore()
    await asyncio.gather(*[
        monitor_tickers(candle_store, last_atr_dict.get(t), ib, t)
        for t in valid_tickers
    ])


# =============================================================================
# Orchestrator
# =============================================================================


async def data_pipe(ib, monitor_set: dict) -> None:
    """
    Data pipeline orchestrator: fetch history -> validate tickers ->
    calculate indicators + persist -> warm up in-memory state -> hand
    off to ``data_streamer`` for the live loop.

    Assumes the top-level orchestrator has already run ``initialize_app``
    (DB pool + PID + dashboard), ``prepare_database`` (all table setup),
    and ``prepare_watchlist`` (monitor-set assembly + dispatcher push).
    Takes the resulting ``monitor_set`` in and drives the pipeline from
    there.
    """
    tickers = sorted(monitor_set)
    daily_data, today_intradaydata, past_intradaydata = await _fetch_history_data(ib, tickers)

    valid_tickers = validate_tickers(
        daily_data, today_intradaydata, past_intradaydata, tickers,
    )
    if not valid_tickers:
        logging.error("No valid tickers found in all datasets. Aborting.")
        return

    relatr_datasets, last_atr_dict = await _calculate_indicators(
        daily_data, today_intradaydata, past_intradaydata,
    )

    # Seed strategies' in-memory state from the enriched history. One-time
    # warmup step: candle timeline + per-strategy overlay metrics + yesterday
    # RTH levels, all before the live streamer starts. Each strategy owns
    # what its own warmup reads from the historical frames.
    warmup.warmup_from_intraday(relatr_datasets)
    warmup.warmup_from_daily(daily_data)

    await data_streamer(ib, valid_tickers, last_atr_dict)


