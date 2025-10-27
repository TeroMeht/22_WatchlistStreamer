
from src.alarms import *

from src.database.db_functions import *

from src.common.calculate import *
from src.common.read_configs_in import *

from .handle_dataframes import *
from .utils import *
from .ibclient import *

from src.strategies import *

# these codes deal with incoming data and run strategies



# candlestore.py
from collections import defaultdict, deque

class CandleStore:
    def __init__(self, max_candles_per_symbol=5000):
        self.candlesticks = defaultdict(lambda: deque(maxlen=max_candles_per_symbol))
        self.minutes_processed = defaultdict(set)

    def get_last(self, symbol):
        """Return last candle or None."""
        if self.candlesticks[symbol]:
            return self.candlesticks[symbol][-1]
        return None

    def append_candle(self, symbol, candle):
        """Add a new candle for a symbol."""
        self.candlesticks[symbol].append(candle)

    def update_candle(self, symbol, price, bar_volume):
        """Update open candle with new tick/bar data."""
        current_candle = self.get_last(symbol)
        if not current_candle:
            return

        # Update OHLC
        current_candle['high'] = max(current_candle['high'], price)
        current_candle['low'] = min(current_candle['low'], price)
        current_candle['close'] = price

        # Add the 5-sec bar volume to the candle
        current_candle['volume'] += bar_volume

    def add_minute(self, symbol, minute_dt):
        self.minutes_processed[symbol].add(minute_dt)

    def seen_minute(self, symbol, minute_dt):
        return minute_dt in self.minutes_processed[symbol]




async def finalize_candle(last_candle,
                          project_config: dict,
                          database_config: dict,
                          atr_value: float,
                          symbol: str,
                          price):
    """Finalize previous candle, enqueue DB write, and run strategy checks."""

    # Update close price
    last_candle['close'] = price

    candle_dt = last_candle["minute_dt"]

    last_candle = [
        symbol,
        candle_dt.date().isoformat(),
        candle_dt.strftime("%H:%M"),
        last_candle["open"],
        last_candle["high"],
        last_candle["low"],
        last_candle["close"],
        last_candle["volume"],  # already cumulative from update_candle()
    ]
    # calculations
    last_candle = handle_next_vwap_and_ema9_values(last_candle, database_config)
    last_candle = calculate_next_relatr(last_candle, atr_value)

    await insert_candlestick_row(last_candle, database_config)

    # run strategy (still in the hot path — you can offload later too)
    await run_strategies(project_config, database_config, last_candle)


async def process_bar(store: CandleStore,
                      project_config: dict,
                      database_config: dict,
                      atr_value: float,
                      symbol: str,
                      bar: 'RealTimeBar'):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.
    """

    interval_time = get_2min_interval(bar.time)


    last_candle = store.get_last(symbol)

    if not store.seen_minute(symbol, interval_time):
        store.add_minute(symbol, interval_time)

        if last_candle:
            # First, apply the last bar to the previous candle
            store.update_candle(symbol, bar.close, bar.volume)
            # Then finalize
            await finalize_candle(last_candle, project_config, database_config, atr_value, symbol, bar.close)

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