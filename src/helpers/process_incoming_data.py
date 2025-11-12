
from src.alarms import *

from src.database.db_functions import *

from src.common.calculate import *
from src.common.read_configs_in import *

from .handle_candles import *
from .handle_barbuffer import *
from .utils import *
from .ibclient import *

from src.strategies import *
from zoneinfo import ZoneInfo
# these codes deal with incoming data and run strategies










async def handle_incoming_candle(candle: CandleRow, atr_value:float ,database_config: dict, database_avgvolume_config: dict) -> CandleRow:
    try:
        df_from_db, avg_volume = await asyncio.gather(
            get_last_rows(table_name=candle.symbol.lower(), num_rows=None, database_config=database_config),
            fetch_avg_volume_for_candle(candle, database_avgvolume_config)
        )
        candle.avg_volume = avg_volume
        candle = calculate_next_vwap(candle, df_from_db)
        candle = calculate_next_ema9(candle, df_from_db)
        candle = calculate_next_relatr(candle, atr_value)
        candle = calculate_next_rvol(candle, candle.avg_volume)
        return candle
    except Exception as e:
        logging.exception("Error in handle_next_vwap_and_ema9_values for %s: %s", candle.symbol, e)
        return candle




async def finalize_candle(last_candle,
                          project_config: dict,
                          database_config: dict,
                          database_avgvolume_config: dict,
                          atr_value: float,
                          symbol: str,
                          price):
    """Finalize previous candle, enqueue DB write, and run strategy checks."""

    # 1️⃣ Update close price
    last_candle['close'] = price

    # 2️⃣ Build CandleRow with known values
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
    last_candle = await handle_incoming_candle(last_candle, atr_value, database_config,database_avgvolume_config)
    



    db_ready_candle = enforce_candle_row_types(last_candle)  # Ensure all floats
    logging.debug(db_ready_candle)

    await insert_candlestick_row(db_ready_candle,database_config)
    # # run strategy (still in the hot path — you can offload later too)
    # await generate_signal_alarm(last_candle,
    #                             "Test Alarm",database_config, project_config)
    await run_strategies(last_candle,project_config, database_config)



async def process_bar(bar_buffer: BarBuffer,
                      store: CandleStore,
                      project_config: dict,
                      database_config: dict,
                      database_avgvolume_config: dict,
                      atr_value: float,
                      symbol: str,
                      bar: 'RealTimeBar'):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.
    """
    # --- Convert time to configured timezone here ---

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(project_config["timezone"]))
    interval_time = get_2min_interval(bar_time_local)
    last_candle = store.get_last(symbol)

    logging.debug(f"New 5-sec bar for {symbol} at {bar_time_local.strftime('%H:%M:%S %Z')}: "
    f"Last= {bar.close}, Volume= {bar.volume}")
    # prepare bar dict
    bar_data = {
        "symbol": symbol,
        "time": bar_time_local,
        "last": bar.close,
        "volume": bar.volume
    }

    # # add to buffer; will auto-flush when batch_size reached
   # await bar_buffer.add(bar_data, insert_bulk_livestream, database_config)

    if not store.seen_minute(symbol, interval_time):
        store.add_minute(symbol, interval_time)

        if last_candle:
            # First, apply the last bar to the previous candle
            store.update_candle(symbol, bar.close, bar.volume)
            # Then finalize
            await finalize_candle(last_candle, project_config, database_config, database_avgvolume_config, atr_value, symbol, bar.close)

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