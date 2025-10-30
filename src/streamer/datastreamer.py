
from src.helpers.ibclient import monitor_tickers
from src.database.db_functions import delete_all_tables_db, create_and_fill_table
import asyncio
import logging
from ib_async import IB

from src.database.db_functions import *
from src.common.read_configs_in import *

from src.helpers.utils import *
from src.strategies import *

from src.helpers.ibclient import *
from src.helpers.handle_dataframes import *
from src.helpers.process_incoming_data import CandleStore
from src.helpers.handle_barbuffer import BarBuffer

async def run_streamer(symbols, project_config, database_config):
    """
    Handles all logic for data fetching, ATR calculations, and live monitoring.
    """

    candle_store = CandleStore()
    bar_buffer = BarBuffer(batch_size=project_config["barbuffer_size"])
    
    logging.info("Cleaning up tables in the database...")
    delete_all_tables_db(database_config)

    ib = IB()
    await ib.connectAsync(project_config["host"],
                          project_config["port"],
                          project_config["clientId"]
    )

    tickers = [s[0] if isinstance(s, tuple) else s for s in symbols]
    time_zone = project_config["timezone"]

    logging.info(f"Fetching 2-min intraday data for {len(tickers)} tickers...")
    intraday_results = await asyncio.gather(*[
        fetch_intraday_history(ib, ticker,time_zone) for ticker in tickers
    ])

    logging.info(f"Fetching 14-day daily historical data for {len(tickers)} tickers...")
    daily_results_with_atr = await asyncio.gather(*[
        fetch_history_daily(ib, ticker) for ticker in tickers
    ])

    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(
       intraday_results, daily_results_with_atr
    )
    logging.info("Relatr calculation completed for all tickers.")

    for ticker, df in relatr_datasets.items():
        create_and_fill_table(df, database_config)

    logging.info("Starting live monitoring...")

    live_tasks = [
        monitor_tickers(bar_buffer,
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