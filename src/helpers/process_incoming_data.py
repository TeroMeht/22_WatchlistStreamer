
from src.alarms import *

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

    await insert_candlestick_row(last_candle, database_config)

    # run strategy (still in the hot path — you can offload later too)
    await run_strategies(project_config, database_config, last_candle)


async def process_bar(
    store: CandleStore,
    project_config: dict,
    database_config: dict,
    atr_value: float,
    symbol: str,
    bar: 'RealTimeBar'
):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.
    """
    bar_time = bar.time + timedelta(hours=3)
    interval_time = get_2min_interval(bar_time)
    close_price, volume = bar.close, bar.volume

    last_candle = store.get_last(symbol)

    if not store.seen_minute(symbol, interval_time):
        store.add_minute(symbol, interval_time)

        if last_candle:
            asyncio.create_task(
                finalize_candle(store, project_config, database_config, atr_value, symbol, close_price)
            )

        store.append_candle(symbol, {
            "minute_dt": interval_time,
            "open": close_price,
            "high": close_price,
            "low": close_price,
            "close": close_price,
            "volume": volume
        })
    else:
        store.update_candle(symbol, close_price, volume)