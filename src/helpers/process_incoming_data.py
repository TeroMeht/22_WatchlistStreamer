
from src.alarms import *

from src.database.db_functions import *

from src.common.calculate import *


from .handle_candles import *
from .handle_barbuffer import *
from .utils import *
from .ibclient import *

from src.strategies import *
from zoneinfo import ZoneInfo







async def handle_incoming_candle(candle: CandleRow, atr_value:float) -> CandleRow:
    try:
        df_from_db, avg_volume = await asyncio.gather(
            get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None),
                          
            fetch_avg_volume_for_candle(candle)
        )
        candle.avg_volume = avg_volume
        candle = calculate_next_vwap(candle, df_from_db)
        candle = calculate_next_ema9(candle, df_from_db)
        candle = calculate_next_relatr(candle, atr_value)
        candle = calculate_next_rvol(candle,df_from_db, candle.avg_volume)
        return candle
    except Exception as e:
        logging.exception("Error in handle_next_vwap_and_ema9_values for %s: %s", candle.symbol, e)
        return candle




async def finalize_candle(last_candle,
                          atr_value: float,
                          symbol: str,
                          price):
    """Finalize previous candle, enqueue DB write, and run strategy checks."""

    # 1️ Update close price
    last_candle['close'] = price

    # 2️ Build CandleRow with known values
    last_candle = CandleRow(symbol=symbol,
                            date=last_candle["minute_dt"].date(),
                            time=last_candle["minute_dt"].time(),
                            open=last_candle["open"],
                            high=last_candle["high"],
                            low=last_candle["low"],
                            close=last_candle["close"],
                            volume=last_candle["volume"],
                            vwap=None,  # to be calculated
                            ema9=None,
                            relatR=None,
                            avg_volume=None,
                            rvol=None
                        )
    # calculations
    last_candle = await handle_incoming_candle(last_candle, atr_value)
    
    db_ready_candle = enforce_candle_row_types(last_candle)  # Ensure all floats
    logger.info(f"Finalized candle for {symbol} at {last_candle.time}: O={last_candle.open}, H={last_candle.high}, L={last_candle.low}, C={last_candle.close}, V={last_candle.volume}, VWAP={last_candle.vwap}, EMA9={last_candle.ema9}, RelatR={last_candle.relatR}, AvgVol={last_candle.avg_volume}, RVol={last_candle.rvol}")
    await insert_candlestick_row(db_ready_candle)
    await run_strategies(last_candle)



async def process_bar(store: CandleStore,
                      atr_value: float,
                      symbol: str,
                      bar: RealTimeBar):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.
    """
    # --- Convert time to configured timezone here ---

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(CLIENT_CONFIG["timezone"]))
    interval_time = get_2min_interval(bar_time_local)
    last_candle = store.get_last(symbol)

    logging.debug(f"New 5-sec bar for {symbol} at {bar_time_local.strftime('%H:%M:%S %Z')}: "
    f"Last= {bar.close}, Volume= {bar.volume}")

    if not store.seen_minute(symbol, interval_time):
        store.add_minute(symbol, interval_time)

        if last_candle:
            # First, apply the last bar to the previous candle
            store.update_candle(symbol, bar.close, bar.volume)
            # Then finalize
            await finalize_candle(last_candle, atr_value, symbol, bar.close)

        # Start a new candle for the current interval
        store.append_candle(symbol, {
            "minute_dt": interval_time,
            "open": bar.close,
            "high": bar.close,
            "low": bar.close,
            "close": bar.close,
            "volume": 0.0
        })
    else:
        # Accumulate volume in the current candle
        store.update_candle(symbol, bar.close, bar.volume)