
from src.helpers.ibclient import monitor_tickers
from src.database.db_functions import archive_livestream_tables, delete_all_tables_db_async
from src.alarms.send_postrequest import send_streamer_status
import asyncio
import logging
import os

from src.database.db_functions import *


from src.helpers.utils import *
from src.streamer.datavalidation import *
from src.strategies.strategies import *

from src.helpers.ibclient import *
from src.helpers.handle_dataframes import *
from src.helpers.process_incoming_data import CandleStore
from src.database.watchlist import load_watchlist, create_watchlist_tables
from src.database.exit_requests import load_armed_exit_strategies
from src.strategies.visualization.dashboard import start_dashboard
from src.strategies.orb_long.visualization import state as viz
from src.strategies.orb_long import state as orb_strategy_state
from src.strategies.reversal_long.visualization import state as reversal_viz
from src.strategies.visualization import chart_state as candle_timeline
from src.core.config import settings


async def run_streamer(ib):
    """
    Handles all logic for data fetching, ATR calculations, and live monitoring.

    Fires one POST to the FastAPI start endpoint as the very first thing so
    the UI dot flips green immediately, and one POST to the stop endpoint
    in a try/finally so a clean shutdown (Ctrl-C, normal exit) flips it
    gray. A hard crash skips the stop signal — known tradeoff.
    """

    candle_store = CandleStore()

    # Send our PID so the backend can watch the process for hard kills.
    await send_streamer_status(
        settings.STREAMER_START_ENDPOINT,
        label="start",
        payload={"pid": os.getpid()},
    )

    # Bring up the unified strategy dashboard (localhost:8790). One page
    # renders overlays from every strategy's viz state module. Non-fatal
    # if aiohttp isn't installed -- start_dashboard logs and returns None.
    await start_dashboard()

    # Archive every existing *_livestream row into bars_2m_archive BEFORE
    # the startup wipe -- so historical + previous-session live bars are
    # kept for later comparison against bars_5s.log.
    await archive_livestream_tables()
    await delete_all_tables_db_async()

    # --- Load watchlist + per-ticker strategy selection from DB --------------
    # Source of truth for entries is the `watchlist` / `watchlist_strategies`
    # tables, written by the 26_ReactFastApp UI. Tables are created on demand
    # inside load_watchlist() so a fresh DB doesn't fail here.
    await create_watchlist_tables()
    watchlist = await load_watchlist()  # {SYMBOL: {strategy_name, ...}}

    # We also need to monitor any symbol that has an armed exit request, even
    # if it's NOT on the watchlist — otherwise an open position with an exit
    # plan but no entry binding would receive no candles and the exit would
    # never fire. Source of truth for exits is the shared `exit_requests`
    # table; symbols there get a synthetic empty entry-strategy set so the
    # entry dispatcher skips them while exits still run normally.
    armed_exits = await load_armed_exit_strategies()
    for symbol in armed_exits.keys():
        watchlist.setdefault(symbol, set())

    if not watchlist:
        logging.warning(
            "Nothing to monitor: watchlist is empty AND no armed exit "
            "requests exist. Add tickers via the UI (POST /api/watchlist) "
            "or arm an exit plan; exiting."
        )
        return

    logging.info(
        "Monitor set: %d symbols (%d from watchlist, %d added from exit_requests)",
        len(watchlist),
        sum(1 for s in watchlist if s not in armed_exits or watchlist[s]),
        sum(1 for s in armed_exits if s in watchlist and not watchlist[s]),
    )

    # Push the mapping into the strategy dispatcher so run_strategies() can
    # filter entry strategies per ticker. Symbols pulled in from exit_requests
    # have an empty entry set, so only exits fire for them.
    set_watchlist_strategies(watchlist)

    tickers = sorted(watchlist.keys())
    

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

    # --- Seed the shared candle timeline with historical 2-min candles.
    # Live 5-sec ticks will animate the next candle after these;
    # finalize_candle keeps appending as the session runs. Both dashboards
    # read from this same timeline, so we seed it once here.
    for symbol, df in relatr_datasets.items():
        if df is None or df.empty:
            continue
        candle_timeline.seed_from_history(symbol, df.to_dict(orient="records"))
        # Per-strategy overlay seeds:
        # ORB: latest Rvol so the "Rvol > 3" check has a value on load.
        if "Rvol" in df.columns:
            viz.record_rvol(symbol, float(df.iloc[-1]["Rvol"]))
        # reversal_long: tail of the historical Relatr column so the
        # "recent capitulation" check reflects state at startup. The
        # writer maintains a rolling window internally.
        if "Relatr" in df.columns:
            for r in df["Relatr"].dropna().tail(reversal_viz.RECENT_RELATR_WINDOW):
                reversal_viz.record_relatr(symbol, float(r))
    logging.info(
        "Candle timeline seeded (%d symbols) + per-strategy overlays warmed up",
        sum(1 for df in relatr_datasets.values() if df is not None and not df.empty),
    )

    # --- Seed yesterday's RTH high + close from the daily fetch. The daily
    # data was pulled with useRTH=True and durationStr='14 D' ending
    # yesterday, so the LAST row per symbol is yesterday's regular-hours
    # session -- premarket is naturally excluded. Used by the dashboard's
    # setup-validation checkboxes below the chart.
    for df in daily_data:
        if df is None or df.empty:
            continue
        symbol = df["Symbol"].iloc[0]
        yrow = df.iloc[-1]
        yhi = float(yrow["High"])
        ycl = float(yrow["Close"])
        # Two sinks: the dashboard (for the checkbox rows) and the strategy
        # state (for the yesterday-level filters). Both are cheap in-memory
        # writes; keeping them separate preserves the strategy/viz split.
        # Single write to the strategy state; the dashboard reads yesterday's
        # values from there via viz.snapshot() so we don't keep two copies.
        orb_strategy_state.record_yesterday_daily(symbol=symbol, high=yhi, close=ycl)

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

