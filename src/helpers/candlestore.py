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