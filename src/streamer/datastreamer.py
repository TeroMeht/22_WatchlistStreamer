
from src.helpers.ibclient import monitor_tickers
from src.database.db_functions import delete_all_tables_db_async
import asyncio
import logging

from src.database.db_functions import *


from src.helpers.utils import *
from src.streamer.datavalidation import *
from src.strategies import *

from src.helpers.ibclient import *
from src.helpers.handle_dataframes import *
from src.helpers.process_incoming_data import CandleStore
from src.symbol_loader.loader import load_symbols_from_folder
from src.core.config import settings


async def run_streamer(ib):
    """
    Handles all logic for data fetching, ATR calculations, and live monitoring.
    """

    candle_store = CandleStore()


    await delete_all_tables_db_async()

    # Load symbols
    symbols = load_symbols_from_folder(settings.TICKERS_FOLDER)


    tickers = [s[0] if isinstance(s, tuple) else s for s in symbols]
    

    tasks = []
    for ticker in tickers:
        tasks.append(fetch_history_daily(ib, ticker))
        tasks.append(fetch_intraday_volume_history(ib, ticker))

    results = await asyncio.gather(*tasks)

    daily_data = results[0::2]   # daily tasks
    intraday_data  = results[1::2]   # this has to be unpacked

    # Unpack intraday results
    today_intradaydata = [r[0] if r else None for r in intraday_data]
    past_intradaydata = [r[1] if r else None for r in intraday_data]


    # --- validate each and combine found tickers ---
    found_intraday = validate_datasets(today_intradaydata, tickers, "2-min intraday")
    found_daily = validate_datasets(daily_data, tickers, "14-day daily")
    found_volume = validate_datasets(past_intradaydata, tickers, "5-day intraday")

    # keep only tickers that were found in all datasets
    valid_tickers = [t for t in tickers if t in found_intraday and t in found_daily and t in found_volume]

    # --- If no valid tickers, stop early ---
    if not valid_tickers:
        logging.error(" No valid tickers found in all datasets. Aborting.")
        return

    valid_results = {"intraday": [], "daily": [],"volume": []}

    for df_list, key in zip([today_intradaydata, daily_data, past_intradaydata], ["intraday", "daily", "volume"]):
        for df in df_list:
            if df is not None and not df.empty and df['Symbol'].iloc[0] in valid_tickers:
                valid_results[key].append(df)

    rvol_dataset = handle_intraday_rvol_dataset(today_intradaydata,past_intradaydata)
    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(rvol_dataset, daily_data)

    await asyncio.gather(
        create_and_fill_avg_volume_tables_async(past_intradaydata),
        *(create_and_fill_table_async(df) for df in relatr_datasets.values())
    )

    # --- Determine and log dropped tickers ---
    dropped_tickers = [t for t in tickers if t not in valid_tickers]


    logging.warning(f"Dropped tickers: {dropped_tickers}")
    logging.info("Starting live monitoring...")
    # --- Start live monitoring tasks ---#    
    live_tasks = [monitor_tickers(candle_store,
                                    last_atr_dict.get(ticker),
                                    ib,
                                    ticker
                                )
        for ticker in valid_tickers
    ]

    await asyncio.gather(*live_tasks)
    