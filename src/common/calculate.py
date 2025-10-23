import pandas as pd
import logging

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

    if 'Close' not in data.columns:
        raise ValueError("The DataFrame must contain a 'Close' column.")

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

def calculate_relatr(intraday_df, daily_atr_df):
    intraday_df = intraday_df.copy()
    
    # Map last ATR per symbol directly
    last_atr = daily_atr_df.groupby('Symbol')['ATR'].last()
    intraday_df['LastATR'] = intraday_df['Symbol'].map(last_atr).fillna(1)
    
    # Vectorized calculation
    intraday_df['Relatr'] = ((intraday_df['VWAP'] - intraday_df['Close']) / intraday_df['LastATR']).round(2)
    intraday_df.drop(columns=['LastATR'], inplace=True)
    
    return intraday_df






# Dynaamiset laskennat sisään tulevalle riville
def calculate_next_vwap(new_row, historical_df):

    try:
        # Ensure numeric types
        df = historical_df.copy()
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Compute OHLC4 for historical data
        if not df.empty:
            df["OHLC4"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4
        else:
            df["OHLC4"] = pd.Series(dtype=float)

        # Compute OHLC4 for new row
        new_row_ohlc4 = (new_row[3] + new_row[4] + new_row[5] + new_row[6]) / 4

        # Cumulative VWAP calculation
        cumulative_volume = df["Volume"].sum() + float(new_row[7])
        cumulative_price_volume = (df["OHLC4"] * df["Volume"]).sum() + (new_row_ohlc4 * float(new_row[7]))

        vwap_value = round(cumulative_price_volume / cumulative_volume, 2) if cumulative_volume != 0 else 0.0

        # Append VWAP to new_row
        new_row.append(float(vwap_value))


        return new_row

    except Exception as e:
        logging.error(f"Error calculating VWAP for {new_row[0]}: {e}")
        new_row.append(0.0)
        return new_row

    except Exception as e:
        logging.error(f"Error in calculate_next_vwap_ema9 for {new_row[0]}: {e}")
        return new_row

def calculate_next_ema9(new_row, historical_df):
    """
    Calculate EMA9 for a new_row based on historical_df using pandas ewm.
    Appends the EMA9 value to new_row.
    """
    try:
        # Build a DataFrame including historical data + new row
        df = historical_df.copy()

        # Ensure 'Close' is numeric
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce").fillna(0)

        # Append new row Close
        new_close = float(new_row[6])  # Close is at index 6
        new_row_df = pd.DataFrame([{"Close": new_close}])
        df = pd.concat([df[["Close"]], new_row_df], ignore_index=True)

        # Calculate EMA9
        df["EMA9"] = df["Close"].ewm(span=9, adjust=False).mean().round(2)

        # Append the latest EMA9 to new_row
        new_row.append(float(df["EMA9"].iloc[-1]))

        return new_row

    except Exception as e:
        logging.error(f"Error calculating EMA9 for {new_row[0]}: {e}")
        new_row.append(0.0)
        return new_row

def calculate_next_relatr(new_row, atr_value):
    """
    Calculate the Relatr value for a new candlestick row and append it to the row.
    """
    try:
        if not atr_value:
            logging.warning(f"ATR value missing or zero for {new_row[0]}")
            new_row.append(None)
            return new_row

        # Relatr = (VWAP - Close) / ATR
        relatr_value = round((new_row[8] - new_row[6]) / atr_value, 2)
        new_row.append(relatr_value)

    except Exception as e:
        logging.error(f"Error calculating Relatr for {new_row[0]}: {e}")
        new_row.append(None)

    return new_row
    




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