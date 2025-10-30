import pandas as pd
import logging
from src.helpers.handle_candles import *
from src.database.db_functions import *


logger = logging.getLogger(__name__)  # module-specific logger


def calculate_vwap(data):
    data = data.copy()
    data['OHLC4'] = (data['Open'] + data['High'] + data['Low'] + data['Close']) / 4
    cumulative_vol = data['Volume'].cumsum()
    cumulative_pv = (data['OHLC4'] * data['Volume']).cumsum()
    data['VWAP'] = (cumulative_pv / cumulative_vol).fillna(0).round(2)
    data.drop(columns=['OHLC4'], inplace=True)
    return data

def calculate_ema(data,period):

    # Calculate EMA9 using pandas' `ewm` method
    data['EMA9'] = data['Close'].ewm(span=period, adjust=False).mean().round(2)
    return data

def calculate_14day_atr_df(data, period=14):
    """
    Calculate 14-day ATR for all rows and return a DataFrame with ATR column.
    Input: DataFrame with at least High, Low, Close columns.
    Output: DataFrame with Prev_Close, TR, and ATR columns added.
    """
    df = data.copy()

    # Previous close
    df['Prev_Close'] = df['Close'].shift(1)

    # True Range (TR)
    df['TR'] = df.apply(
        lambda row: max(
            row['High'] - row['Low'],
            abs(row['High'] - row['Prev_Close']) if pd.notnull(row['Prev_Close']) else row['High'] - row['Low'],
            abs(row['Low'] - row['Prev_Close']) if pd.notnull(row['Prev_Close']) else row['High'] - row['Low']
        ),
        axis=1
    )

    # ATR: exponential moving average of TR (rounded to 4 decimals)
    df['ATR'] = df['TR'].ewm(span=period, adjust=False).mean().round(4)

    return df

def calculate_relatr(intraday_df: pd.DataFrame, 
                     last_atr_per_symbol: dict) -> pd.DataFrame:
    """
    Calculate Relatr for a single intraday DataFrame using a dictionary of last ATR per symbol.

    Relatr = (VWAP - Close) / ATR
    """
    intraday_df = intraday_df.copy()
    # Map ATR for each row based on Symbol
    intraday_df['Relatr'] = intraday_df['Symbol'].map(last_atr_per_symbol).fillna(1)
    intraday_df['Relatr'] = ((intraday_df['VWAP'] - intraday_df['Close']) / intraday_df['Relatr']).round(2)
    return intraday_df


def calculate_next_vwap(candle: CandleRow, historical_df: pd.DataFrame) -> CandleRow:
    try:
        df = historical_df.copy()
        df["OHLC4"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4

        new_ohlc4 = (candle.open + candle.high + candle.low + candle.close) / 4
        cumulative_volume = df["Volume"].sum() + candle.volume
        cumulative_price_volume = (df["OHLC4"] * df["Volume"]).sum() + (new_ohlc4 * candle.volume)

        candle.vwap = round(cumulative_price_volume / cumulative_volume, 2) if cumulative_volume else 0.0

    except Exception as e:
        logging.exception("Error calculating VWAP for %s: %s", candle.symbol, e)
        candle.vwap = 0.0

    return candle


def calculate_next_ema9(candle: CandleRow, historical_df: pd.DataFrame) -> CandleRow:
    try:
        df = historical_df.copy()

        new_row_df = pd.DataFrame([{"Close": candle.close}])
        df = pd.concat([df[["Close"]], new_row_df], ignore_index=True)

        df["EMA9"] = df["Close"].ewm(span=9, adjust=False).mean().round(2)

        candle.ema9 = float(df["EMA9"].iloc[-1])

    except Exception as e:
        logging.exception("Error calculating EMA9 for %s: %s", candle.symbol, e)
        candle.ema9 = 0.0

    return candle


def calculate_next_relatr(candle: CandleRow, atr_value: float) -> CandleRow:
    try:
        candle.relatR = round((candle.vwap - candle.close) / atr_value, 2)

    except Exception as e:
        logging.exception("Error calculating RelATR for %s: %s", candle.symbol, e)
        candle.relatR = 0.0

    return candle





# Laskee positio koon kun tiedetään nämä
def calculate_position_size(entry_price, stop_price, risk):

    try:
        risk_per_unit = entry_price - stop_price
        if risk_per_unit == 0:
            raise ValueError("Entry price and stop price cannot be the same.")
        
        position_size = abs(int(risk / risk_per_unit))  # force integer
        return position_size
    
    except Exception as e:
        logging.error("Error calculating position size:", e)
        return None