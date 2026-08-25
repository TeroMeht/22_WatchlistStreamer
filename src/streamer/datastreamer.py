"""
Warmup -> validate -> indicators -> persist -> seed strategies -> live.

The pipeline is source-agnostic: it takes an injected ``WarmupSource``
and ``LiveSource`` and does NOT know whether the bars come from IB,
Polygon, or CSV replay. Those decisions are made by
``make_warmup_source`` / ``make_live_source`` in ``startup.py``.
"""
from __future__ import annotations

import asyncio
import logging

from src.database.db_functions import (
    create_and_fill_avg_volume_tables_async,
    create_and_fill_table_async,
)
from src.helpers.handle_dataframes import (
    handle_Atr_intraday_dataset,
    handle_intraday_rvol_dataset,
)
from src.strategies              import warmup
from src.streamer.live_source    import LiveSource
from src.streamer.warmup_source  import WarmupSource




# Ticker must exist in all three datasets (daily / today-so-far / past 5 sessions) to be valid.
def _validate_tickers(
    daily: dict,
    today_intra: dict,
    past_intra: dict,
    tickers: list,
) -> list:

    valid_tickers = [
        ticker
        for ticker in tickers
        if ticker in daily
        and ticker in today_intra
        and ticker in past_intra
    ]

    dropped_tickers = [
        ticker for ticker in tickers
        if ticker not in valid_tickers
    ]

    if dropped_tickers:
        logging.warning(
            "Dropped %d tickers due to missing datasets: %s",
            len(dropped_tickers),
            ", ".join(dropped_tickers),
        )

    return valid_tickers





# =============================================================================
# Phase 6 -- indicator calculations (Rvol / ATR / Relatr)
# =============================================================================


def _calculate_indicators(daily_data: list, today_intradaydata: list, past_intradaydata: list) -> tuple:
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

    await asyncio.gather(
        create_and_fill_avg_volume_tables_async(list(past_intradaydata.values())),
        *(create_and_fill_table_async(df) for df in relatr_datasets.values()),
    )


# =============================================================================
# Orchestrator
# =============================================================================


async def data_pipe(warmup_source: WarmupSource, live_source: LiveSource, monitor_set:   dict) -> None:

    tickers = sorted(monitor_set)

    # Fetch warmup DataFrames (daily / today-so-far / past 5 sessions).
    # The concrete source is hidden -- both branches return the same shape.
    daily_data, today_intradaydata, past_intradaydata = await warmup_source.fetch(tickers)

    # This will make sure that all three datasets have the same tickers, and log any that are missing.
    valid_tickers = _validate_tickers(daily_data, today_intradaydata, past_intradaydata, tickers)

    if not valid_tickers:
        logging.error("No valid tickers found in all datasets. Aborting.")
        return

    relatr_datasets, last_atr_dict = _calculate_indicators(daily_data, today_intradaydata, past_intradaydata)

    await _fill_database_tables_with_enriched_data(relatr_datasets, past_intradaydata)

    # Seed strategies' in-memory state from the enriched history. One-time.
    warmup.warmup_from_intraday(relatr_datasets)
    warmup.warmup_from_daily(daily_data)

    # Hand off to the live streamer (or CSV replay -- same interface).
    await live_source.run(valid_tickers, last_atr_dict)
