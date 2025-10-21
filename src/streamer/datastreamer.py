from src.helpers.candlestore import CandleStore
from src.helpers.ibclient import monitor_tickers
from src.database.db_functions import delete_all_tables_db, create_and_fill_table
import asyncio
import logging
from ib_async import IB

from src.database.db_functions import *
from src.common.read_configs_in import *

from src.helpers.utils import *
from src.strategies import *
from src.helpers.candlestore import *
from src.helpers.ibclient import *
from src.helpers.handle_dataframes import *



async def run_streamer(symbols, project_config, database_config):
    """
    Handles all logic for data fetching, ATR calculations, and live monitoring.
    """

    candle_store = CandleStore()
    logging.info("Cleaning up tables in the database...")
    delete_all_tables_db(database_config)

    ib = IB()
    await ib.connectAsync(        project_config.get("host"),
        project_config.get("port"),
        project_config.get("client_id")
    )

    tickers = [s[0] if isinstance(s, tuple) else s for s in symbols]

    logging.info(f"Fetching 2-min intraday data for {len(tickers)} tickers...")
    intraday_results = await asyncio.gather(*[
        fetch_intraday_history(ib, ticker) for ticker in tickers
    ])

    logging.info(f"Fetching 14-day daily historical data for {len(tickers)} tickers...")
    daily_results = await asyncio.gather(*[
        fetch_history_daily(ib, ticker) for ticker in tickers
    ])

    relatr_datasets = handle_Atr_intraday_dataset(tickers, intraday_results, daily_results)
    logging.info("Relatr calculation completed for all tickers.")

    for ticker, df in relatr_datasets.items():
        create_and_fill_table(df, database_config)

    last_atr_dict = build_last_atr_dict(tickers, daily_results)
    logging.info("Starting live monitoring...")

    live_tasks = [
        monitor_tickers(
            candle_store,
            project_config,
            database_config,
            last_atr_dict.get(ticker),
            ib,
            ticker
        )
        for ticker in tickers
    ]

    await asyncio.gather(*live_tasks)
    ib.disconnect()