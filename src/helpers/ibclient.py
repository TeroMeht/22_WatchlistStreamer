import logging


import asyncio
from datetime import datetime, timedelta
from ib_async import *
from .process_incoming_data import process_bar
from .handle_dataframes import *

from zoneinfo import ZoneInfo

HELSINKI_TZ = ZoneInfo("Europe/Helsinki")

logger = logging.getLogger(__name__)  # module-specific logger

# Tänne tulee IB:n kanssa asioivat koodit


# Fetch last ask price in order to calculate position size with my risk
async def get_last_ask_price(ib: IB, symbol: str, exchange: str = "SMART", currency: str = "USD", wait_time: float = 0.5) -> float:
    """
    Async function to get the last ask price from IB.
    If ask price is -1 or None, returns the last close price instead.
    """
    # Qualify contract asynchronously
    contract = Stock(symbol=symbol, exchange=exchange, currency=currency)
    await ib.qualifyContractsAsync(contract)

    # Request market data
    ticker = ib.reqMktData(contract, "", False, False)

    # Wait asynchronously instead of ib.sleep
    await asyncio.sleep(wait_time)

    ask_price = ticker.ask

    if ask_price is None or ask_price == -1:
        # fallback to last close price
        last_close = ticker.close
        if last_close is None:
            raise ValueError(f"No valid ask or close price available for {symbol}")
        logging.info(f"Ask price unavailable, using last close for {symbol}: {last_close}")
        return last_close

    logging.info(f"Last ask price for {symbol}: {ask_price}")
    return ask_price

# Fetch history for ATR calculation
async def fetch_history_daily(ib: IB, symbol: str):
    """Fetch last 14 days of daily historical data up to yesterday (needed for ATR)."""
    contract = Stock(symbol, "SMART", "USD")

    # IB format: 'YYYYMMDD HH:MM:SS'
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d 23:59:59')

    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=yesterday,      # stop at yesterday
        durationStr="14 D",         # last 14 calendar days
        barSizeSetting="1 day",     # daily bars
        whatToShow="TRADES",
        useRTH=True                 # only regular trading hours
    )

    if not bars:
        logging.warning(f"No historical data returned for {symbol}")
        return None

    atr_df = handle_incoming_dataframe_daily(bars, symbol)
    return atr_df

# --- Historical fetch ---
async def fetch_intraday_history(ib: IB, symbol: str):
    logging.info(f"Requesting data for {symbol}")

    contract = Stock(symbol, "SMART", "USD")
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime="",
        durationStr="1 D",
        barSizeSetting="2 mins",
        whatToShow="TRADES",
        useRTH=False
    )

    if not bars:
        logging.warning(f"No historical data returned for {symbol}")
        return None

    # Process bars directly using the intraday handler
    processed_df = handle_incoming_dataframe_intraday(bars, symbol)

    return processed_df
        



# --- Real-time monitoring loop ---
async def monitor_tickers(  candle_store,
                            project_config,
                            database_config,
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
                        # Convert bar.time (which is UTC) to Helsinki local time
                bar.time = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(HELSINKI_TZ)

                logging.debug(
                    f"New 5-sec bar for {symbol} at {bar.time.strftime('%Y-%m-%d %H:%M:%S %Z')}: "
                    f"Close={bar.close}, Volume={bar.volume}"
                )
                await process_bar(
                                candle_store,
                                project_config,
                                database_config, 
                                atr,
                                symbol, 
                                bar) 
 
                
    ticker.updateEvent += on_bar # tarkoittaa että kun uusi bar tulee sisään kutsu tätä funktiota

    # keep this coroutine alive indefinitely
    await asyncio.Event().wait()
