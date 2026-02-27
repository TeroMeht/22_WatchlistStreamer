
from src.helpers.ibclient import monitor_tickers
from src.database.db_functions import delete_all_tables_db_async
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
from src.symbol_loader.loader import load_symbols_from_folder
from config import CLIENT_CONFIG


async def run_streamer():
    """
    Handles all logic for data fetching, ATR calculations, and live monitoring.
    """

    candle_store = CandleStore()
    bar_buffer = BarBuffer(batch_size=CLIENT_CONFIG["barbuffer_size"])
    
    logging.info("Cleaning up tables in the database...")
    await delete_all_tables_db_async()

    # Load symbols
    symbols = load_symbols_from_folder(CLIENT_CONFIG["tickers_folder"])


    ib = IB()
    await ib.connectAsync(CLIENT_CONFIG["host"],
                          CLIENT_CONFIG["port"],
                          CLIENT_CONFIG["clientId"])

    tickers = [s[0] if isinstance(s, tuple) else s for s in symbols]
    time_zone = CLIENT_CONFIG["timezone"]

    tasks = []
    for ticker in tickers:
        tasks.append(fetch_intraday_history(ib, ticker, time_zone))
        tasks.append(fetch_history_daily(ib, ticker))
        tasks.append(fetch_intraday_volume_history(ib, ticker, time_zone))

    results = await asyncio.gather(*tasks)
    # Then split results back into intraday, daily, volume
    intraday_results = results[0::3]
    daily_results_with_atr = results[1::3]
    avg_volume_results_5d = results[2::3]



    # --- validate each and combine found tickers ---
    found_intraday = validate_datasets(intraday_results, tickers, "2-min intraday")
    found_daily = validate_datasets(daily_results_with_atr, tickers, "14-day daily")
    found_volume = validate_datasets(avg_volume_results_5d, tickers, "5-day intraday")

    # keep only tickers that were found in all datasets
    valid_tickers = [t for t in tickers if t in found_intraday and t in found_daily and t in found_volume]


    # --- If no valid tickers, stop early ---
    if not valid_tickers:
        logging.error(" No valid tickers found in all datasets. Aborting.")
        ib.disconnect()
        return

    valid_results = {
        "intraday": [],
        "daily": [],
        "volume": []
    }

    for df_list, key in zip([intraday_results, daily_results_with_atr, avg_volume_results_5d], ["intraday", "daily", "volume"]):
        for df in df_list:
            if df is not None and not df.empty and df['Symbol'].iloc[0] in valid_tickers:
                valid_results[key].append(df)

    
    rvol_dataset = handle_intraday_rvol_dataset(intraday_results,avg_volume_results_5d)
    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(rvol_dataset, daily_results_with_atr)

    await create_and_fill_avg_volume_tables_async(avg_volume_results_5d)

    await asyncio.gather(*(create_and_fill_table_async(df) for df in relatr_datasets.values())
)

    # --- Determine and log dropped tickers ---
    dropped_tickers = [t for t in tickers if t not in valid_tickers]


    logging.warning(f"Dropped tickers: {dropped_tickers}")
    logging.info("Starting live monitoring...")
    # --- Start live monitoring tasks ---#    
    live_tasks = [
        monitor_tickers(bar_buffer,
            candle_store,
            CLIENT_CONFIG,
            last_atr_dict.get(ticker),
            ib,
            ticker
        )
        for ticker in valid_tickers
    ]

    await asyncio.gather(*live_tasks)
    ib.disconnect()