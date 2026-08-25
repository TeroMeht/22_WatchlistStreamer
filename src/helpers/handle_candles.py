from dataclasses import dataclass
from datetime import date, time, datetime
import logging
# candlestore.py
from collections import defaultdict, deque
from datetime import datetime

from data_sources._bar import IncomingBar

logger = logging.getLogger(__name__)  # module-specific logger


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


@dataclass
class CandleRow:
    symbol: str
    date: date
    time: time
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    ema9: float
    avg_volume: float
    rvol: float
    relatR: float
    day_atr_ext: float


def enforce_candle_row_types(candle: CandleRow) -> CandleRow:
    """Ensure all fields match the dataclass types (float for numerics)."""
    return CandleRow(
        symbol=candle.symbol,
        date=candle.date,
        time=candle.time,
        open=float(candle.open),
        high=float(candle.high),
        low=float(candle.low),
        close=float(candle.close),
        volume=float(candle.volume),
        vwap=float(candle.vwap),
        ema9=float(candle.ema9),
        avg_volume=float(candle.avg_volume),
        rvol=float(candle.rvol),
        relatR=float(candle.relatR),
        day_atr_ext=float(candle.day_atr_ext),

    )


def stream_data_to_candle_row(symbol: str, bar: IncomingBar, bar_time_local: datetime) -> CandleRow:
    """
    Minimal CandleRow synthesized from a 5-sec bar. Indicator fields
    (vwap/ema9/relatR/rvol/avg_volume) aren't meaningful for a 5-sec
    bar so we zero them; only symbol/date/time/close matter for the
    downstream alarm + order writers.
    """
    close_px = float(bar.close)
    return CandleRow(
        symbol=symbol,
        date=bar_time_local.date(),
        time=bar_time_local.time(),
        open=close_px,
        high=close_px,
        low=close_px,
        close=close_px,
        volume=float(bar.volume),
        vwap=0.0,
        ema9=0.0,
        avg_volume=0.0,
        rvol=0.0,
        relatR=0.0,
        day_atr_ext=0.0,
    )
