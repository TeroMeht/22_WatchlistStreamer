# candlestore.py
from collections import defaultdict, deque

class CandleStore:
    def __init__(self, max_candles_per_symbol=5000):
        # in-memory state per symbol
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

    def update_candle(self, symbol, price, size):
        """Update open candle with new tick data."""
        current_candle = self.get_last(symbol)
        if not current_candle:
            return
        current_candle['high'] = max(current_candle['high'], price)
        current_candle['low'] = min(current_candle['low'], price)
        current_candle['close'] = price
        current_candle['volume'] += size

    def add_minute(self, symbol, minute_dt):
        self.minutes_processed[symbol].add(minute_dt)

    def seen_minute(self, symbol, minute_dt):
        return minute_dt in self.minutes_processed[symbol]