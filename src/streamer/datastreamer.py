"""
Data pipeline + live streamer.

``data_pipe`` orchestrates the one-shot pipeline (fetch history ->
validate tickers -> calculate indicators -> persist -> warmup) and hands
off to ``run_streamer`` for the live loop. Startup phases
(``initialize_app`` / ``prepare_database`` / ``prepare_watchlist``) live
in ``src.streamer.startup`` -- ``main.py`` runs those first and passes
the assembled ``monitor_set`` in here.
"""

from __future__ import annotations

import asyncio
import logging

from src.core.config import settings
from src.database.db_functions import (
    create_and_fill_avg_volume_tables_async,
    create_and_fill_table_async,
)
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
from src.streamer import replay
from src.streamer.datavalidation import validate_tickers


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

    Returns three dicts keyed by Symbol:
    ``(daily, today_intra, past_intra)``. Only successful, non-empty
    frames are included -- failed fetches simply drop out, so nothing
    downstream ever has to guard against ``None`` frames. The three
    dicts may cover different ticker subsets; ``validate_tickers``
    intersects them.
    """
    daily_results, intraday_pairs = await asyncio.gather(
        asyncio.gather(*(fetch_history_daily(ib, t) for t in tickers)),
        asyncio.gather(*(fetch_intraday_volume_history(ib, t) for t in tickers)),
    )

    def _ok(df) -> bool:
        return df is not None and not df.empty

    daily = {t: d for t, d in zip(tickers, daily_results) if _ok(d)}
    today = {t: p[0] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[0])}
    past  = {t: p[1] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[1])}
    return daily, today, past


# =============================================================================
# Phase 6 -- indicator calculations (Rvol / ATR / Relatr)
# =============================================================================


def _calculate_indicators(
    daily_data: list,
    today_intradaydata: list,
    past_intradaydata: list,
) -> tuple:
    """
    Enrich the raw history with the derived columns strategies need. Pure
    computation, no I/O -- persistence is a separate phase.

        * ``handle_intraday_rvol_dataset`` -- compute Rvol from today's
          2-min bars against the 5-day intraday-volume model.
        * ``handle_Atr_intraday_dataset`` -- compute ATR from daily bars,
          join it in as ``Relatr`` on the intraday frame, and expose the
          latest per-ticker ATR for use as the stop-distance seed.

    Returns ``(relatr_datasets, last_atr_dict)``. ``relatr_datasets`` is a
    dict ``{SYMBOL: DataFrame}`` of the enriched intraday history, fed to
    both the persistence phase and the warmup step.
    """
    rvol_dataset = handle_intraday_rvol_dataset(today_intradaydata, past_intradaydata)
    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(rvol_dataset, daily_data)
    return relatr_datasets, last_atr_dict


# =============================================================================
# Phase 7 -- persist indicator datasets
# =============================================================================


async def _fill_database_tables_with_enriched_data(
    relatr_datasets: dict,
    past_intradaydata: dict,
) -> None:
    """
    Write the enriched intraday history to the DB tables the live path
    reads from:
        * average-volume tables (one per ticker) written from the 5-day
          intraday-volume frames -- read by the live path to compute Rvol
          per finalized 2-min candle.
        * per-ticker ``*_livestream`` tables seeded with the enriched
          intraday history -- the base rows every strategy reads from.

    Both sets of writes run concurrently.
    """
    await asyncio.gather(
        create_and_fill_avg_volume_tables_async(list(past_intradaydata.values())),
        *(create_and_fill_table_async(df) for df in relatr_datasets.values()),
    )


# =============================================================================
# Phase 8 -- live data streamer
# =============================================================================


async def run_streamer(ib, valid_tickers: list, last_atr_dict: dict) -> None:
    """
    Subscribe to the live 5-sec bar stream for every valid ticker and
    dispatch each incoming bar through the strategy pipeline (via
    ``monitor_tickers``). Runs until the process is stopped; each
    ticker's subscription is one gathered coroutine, so cancellation of
    one doesn't cancel the others until the outer task is cancelled.

    ``last_atr_dict`` seeds each ticker's per-tick ATR helper --
    ``monitor_tickers`` refreshes it as new 2-min candles finalize.

    When ``settings.MODE == "replay"`` this instead hands off to the
    CSV replay driver, which feeds bars from disk through the same
    ``process_bar`` pipeline. Everything up to this point (history
    fetch, indicator warmup, DB seeding) still runs as normal so the
    replay starts against the same in-memory state a live session would.
    """
    candle_store = CandleStore()

    if settings.MODE == "replay":
        logging.info("Starting replay mode (speed=%s, data_dir=%s)...",
                     settings.REPLAY_SPEED, settings.REPLAY_DATA_DIR)
        await replay.run_replay(candle_store, last_atr_dict, valid_tickers)
        return

    logging.info("Starting live monitoring...")
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
    filter to valid frames -> calculate indicators -> persist -> warm up
    in-memory state -> hand off to ``run_streamer`` for the live loop.

    Assumes the startup phases (``initialize_app``, ``prepare_database``,
    ``prepare_watchlist``) have already run. Takes the assembled
    ``monitor_set`` in and drives the pipeline from there.
    """
    tickers = sorted(monitor_set)
    # _fetch_history_data returns dicts keyed by Symbol, containing only
    # successful non-empty frames -- nothing downstream ever needs to
    # guard against None. validate_tickers intersects the three dicts.
    daily_data, today_intradaydata, past_intradaydata = await _fetch_history_data(ib, tickers)

    valid_tickers, daily_data, today_intradaydata, past_intradaydata = validate_tickers(
        daily_data, today_intradaydata, past_intradaydata, tickers,
    )
    if not valid_tickers:
        logging.error("No valid tickers found in all datasets. Aborting.")
        return

    relatr_datasets, last_atr_dict = _calculate_indicators(
        daily_data, today_intradaydata, past_intradaydata,
    )

    await _fill_database_tables_with_enriched_data(relatr_datasets, past_intradaydata)

    # Seed strategies' in-memory state from the enriched history. One-time

    warmup.warmup_from_intraday(relatr_datasets)
    warmup.warmup_from_daily(daily_data)

    await run_streamer(ib, valid_tickers, last_atr_dict)
