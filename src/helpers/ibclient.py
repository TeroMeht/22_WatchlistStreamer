import logging


import asyncio
from datetime import datetime, timedelta
from ib_async import *
from src.core.config import settings
from .process_incoming_data import process_bar
from .handle_dataframes import *




logger = logging.getLogger(__name__)  # module-specific logger

# Tänne tulee IB:n kanssa asioivat koodit

# Fetch history for ATR calculation
async def fetch_history_daily(ib: IB, symbol: str):
    logging.info(f"Requesting 14 daily historical data for {symbol}")

    contract = Stock(symbol, "SMART", "USD")
    await ib.qualifyContractsAsync(contract)

    # Anchor "end of window" to the day BEFORE the replay's session in
    # replay mode, otherwise wall-clock yesterday. IB format: 'YYYYMMDD HH:MM:SS'.
    if settings.MODE == "replay":
        # Local import so live mode never pulls the replay module.
        from src.streamer.replay import get_replay_start_datetime
        anchor_date = get_replay_start_datetime().date() - timedelta(days=1)
    else:
        anchor_date = (datetime.now() - timedelta(days=1)).date()
    end_dt_str = anchor_date.strftime('%Y%m%d 23:59:59')

    bars = await ib.reqHistoricalDataAsync( contract,
                                            endDateTime=end_dt_str,     # stop at yesterday (or replay-1)
                                            durationStr="14 D",         # last 14 calendar days
                                            barSizeSetting="1 day",     # daily bars
                                            whatToShow="TRADES",
                                            useRTH=True                 # only regular trading hours
                                        )
    await asyncio.sleep(1)
    if not bars:
        logging.warning(f"No historical data returned for {symbol}")
        return None

    atr_df = handle_incoming_dataframe_daily(bars, symbol)
    return atr_df


        
# --- Historical fetch for Rvol calculation ---
async def fetch_intraday_volume_history(ib: IB, symbol: str):
    logging.info(f"Requesting Rvol data for {symbol}")

    contract = Stock(symbol, "SMART", "USD")
    await ib.qualifyContractsAsync(contract)

    # Live mode: endDateTime="" means "now" per IB docs.
    # Replay mode: end at the replay's session start (typically 16:30
    # Helsinki = US market open) so the returned 5 days of 2-min bars
    # stop right at the moment the replay picks up. Pass a tz-aware
    # datetime -- ib_async converts it to the exact IB wire format,
    # which is safer than hand-building a "YYYYMMDD HH:MM:SS <tz>"
    # string (arbitrary IANA tz names aren't always accepted).
    if settings.MODE == "replay":
        from src.streamer.replay import get_replay_start_datetime
        end_dt = get_replay_start_datetime()
    else:
        end_dt = ""

    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=end_dt,
        durationStr="5 D",
        barSizeSetting="2 mins",
        whatToShow="TRADES",
        useRTH=False
    )
    await asyncio.sleep(1)
    if not bars:
        logging.warning(f"No 5-day historical data returned for {symbol}")
        return None

    # Process bars directly using the intraday handler
    today_df, past_df = handle_incoming_dataframe_intradays_volume(bars, symbol)

    return today_df,past_df

# --- Real-time monitoring loop ---
async def monitor_tickers( candle_store,
                            atr,
                            ib: IB, 
                            symbol: str):
    
    """Subscribe to 5-sec real-time bars and aggregate into 2-min candles."""
    ticker = ib.reqRealTimeBars(
        Stock(symbol, "SMART", "USD"),
        barSize=5,
        whatToShow="TRADES",
        useRTH=False
    )

    async def on_bar(bars: list[RealTimeBar], hasNewBar: bool):
            if hasNewBar and bars:
                bar = bars[-1]
                await process_bar(candle_store,
                                atr,
                                symbol, 
                                bar) 
 
    ticker.updateEvent += on_bar # tarkoittaa että kun uusi bar tulee sisään kutsu tätä funktiota

    # keep this coroutine alive indefinitely
    await asyncio.Event().wait()
