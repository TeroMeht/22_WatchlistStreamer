
from src.alarm_logics import *

from src.database.db_functions import *

from src.common.calculate import *
from src.common.read_configs_in import *

from .handle_dataframes import *
from .utils import *
from .candlestore import *
from .ibclient import *

from src.strategies import *

# these codes deal with incoming data and run strategies



async def finalize_candle(store: CandleStore,
                          project_config,
                          database_config,
                          atr_value,
                          symbol,
                          price
                          ):
    """Finalize previous candle, enqueue DB write, and run strategy checks."""
    prev_candle = store.get_last(symbol)
    if not prev_candle:
        return

    prev_candle['close'] = price
    candle_dt = prev_candle["minute_dt"]

    last_candle = [
        symbol,
        candle_dt.date().isoformat(),
        candle_dt.strftime("%H:%M"),
        prev_candle["open"],
        prev_candle["high"],
        prev_candle["low"],
        prev_candle["close"],
        prev_candle["volume"],
    ]

    # calculations
    last_candle = handle_next_vwap_and_ema9_values(last_candle, database_config)
    last_candle = calculate_next_relatr(last_candle, atr_value)

    save_candlestick_row_to_db(last_candle, database_config)

    # run strategy (still in the hot path — you can offload later too)
    run_strategies(project_config, database_config, last_candle)


async def process_bar(store: CandleStore,
                      project_config,
                      database_config,
                      atr_value,
                      symbol: str,
                      bar: 'RealTimeBar'
                      ):
    """Process incoming 5-sec bar into aggregated 2-min candlesticks."""

    time_obj = bar.time + timedelta(hours=3)
    rounded_time = get_2min_interval(time_obj)
    price, size = bar.close, bar.volume

    if not store.seen_minute(symbol, rounded_time):
        store.add_minute(symbol, rounded_time)

        if store.get_last(symbol):
            await finalize_candle(store, project_config, database_config,
                                  atr_value, symbol, price)

        store.append_candle(symbol, {
            "minute_dt": rounded_time,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": size
        })
    else:
        store.update_candle(symbol, price, size)