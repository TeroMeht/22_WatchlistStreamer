from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass
from datetime import date, time, datetime




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
    relatR: float

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
        relatR=float(candle.relatR)
    )

def get_2min_interval(dt: datetime) -> datetime:
    """Round datetime down to nearest 2-minute interval."""
    minute = (dt.minute // 2) * 2
    return dt.replace(second=0, microsecond=0, minute=minute)