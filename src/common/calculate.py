import pandas as pd
import logging
from src.helpers.handle_candles import *
import numpy as np

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

    # True Range (TR) -- vectorized.
    # Semantics identical to the previous row-wise df.apply() implementation:
    # when Prev_Close is NaN (first row), the High-Prev_Close and
    # Low-Prev_Close components fall back to (High - Low). We reproduce this
    # by filling NaN values in the shifted reference with the current High
    # for the H-PC term and the current Low for the L-PC term, which makes
    # both absolute differences collapse to (High - Low) for that row.
    hl = df['High'] - df['Low']
    h_pc = (df['High'] - df['Prev_Close'].fillna(df['High'])).abs()
    l_pc = (df['Low'] - df['Prev_Close'].fillna(df['Low'])).abs()
    df['TR'] = np.maximum.reduce([hl.values, h_pc.values, l_pc.values])

    # ATR: exponential moving average of TR (rounded to 4 decimals)
    df['ATR'] = df['TR'].ewm(span=period, adjust=False).mean().round(4)

    return df

def calculate_relatr(intraday_df: pd.DataFrame, last_atr_per_symbol: dict) -> pd.DataFrame:
    """
    Calculate Relatr for a single intraday DataFrame using a  dictionary of last ATR per symbol.

    Relatr = (VWAP - Close) / ATR
    """
    intraday_df = intraday_df.copy()
    # Map ATR for each row based on Symbol
    intraday_df['Relatr'] = intraday_df['Symbol'].map(last_atr_per_symbol).fillna(1)
    intraday_df['Relatr'] = ((intraday_df['VWAP'] - intraday_df['Close']) / intraday_df['Relatr']).round(2)
    return intraday_df


def calculate_day_atr_ext(
    intraday_df: pd.DataFrame,
    last_atr_per_symbol: dict,
    last_prev_close_per_symbol: dict,
) -> pd.DataFrame:
    """
    Day-level ATR extension from yesterday's close.

    DayAtrExt = (Prev_Close - Close) / ATR

    Positive => price is below yesterday's close (bearish extension),
    matching Relatr's "positive = below reference" sign convention.
    Captures the full move including any pre/after-market gap, unlike
    Relatr which anchors on today's intraday VWAP.
    """
    intraday_df = intraday_df.copy()
    atr        = intraday_df['Symbol'].map(last_atr_per_symbol).fillna(1)
    prev_close = intraday_df['Symbol'].map(last_prev_close_per_symbol)
    intraday_df['DayAtrExt'] = ((prev_close - intraday_df['Close']) / atr).round(2)
    return intraday_df

# Winsorization ceiling for the per-slot volume sample used by the
# ``calculate_avg_volume_model`` baseline. Any single-session per-slot
# volume greater than ``AVG_VOLUME_WINSOR_K * median(slot)`` is clipped
# down to that ceiling BEFORE the slot mean is computed.
#
# Motivation -- the "shifting baseline" problem:
# Rvol = today_volume / avg_of_past_N_sessions. If one of those past
# sessions was a genuine spike day (catalyst, earnings, sympathy move),
# its huge volume drags the average up, and the NEXT day's still-
# elevated (but not extreme) volume divides into an inflated denominator
# and falls below the Rvol>=3 filter. Continuation opportunities get
# silently suppressed by yesterday's event.
#
# Median-based cap because the median is robust by construction: the
# very spike we want to neutralize cannot distort its own ceiling.
# k=3 leaves ordinary "busy" days untouched (a 2-3x-normal day lands
# within the cap and is averaged in as-is) but pulls a 10x spike back
# to 3x median before it enters the mean. Slots whose median is zero
# (dead pre/post-market intervals) are skipped -- clipping there would
# be a no-op anyway.
AVG_VOLUME_WINSOR_K: float = 3.0


def calculate_avg_volume_model(day5_history_datas: pd.DataFrame) -> pd.DataFrame:
    """
    Combine 5 days of intraday data and calculate the average volume
    for each Symbol-Time combination.

    Per-slot volumes are WINSORIZED before averaging: any single
    session's volume in a (Symbol, Time) bucket that exceeds
    ``AVG_VOLUME_WINSOR_K`` times the bucket's median is clipped down
    to that ceiling. See ``AVG_VOLUME_WINSOR_K`` above for the full
    rationale. This is what lets a day-after-a-spike stock still fire
    Rvol>=3 in continuation.

    Parameters
    ----------
    day5_history_datas : list[pd.DataFrame]
        List of 5 daily DataFrames with columns:
        ['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']

    Returns
    -------
    pd.DataFrame
        A single DataFrame (average day model) with columns:
        ['Symbol', 'Time', 'Avg_volume']
    """
    # Combine all sessions.
    all_data = pd.concat(day5_history_datas, ignore_index=True)

    # Winsorize per (Symbol, Time) slot. Vectorized via groupby.transform
    # so the whole pass is one median compute + one mask + one assign.
    # Median-zero slots keep their original values (clipping would be a
    # no-op there anyway).
    grp_vol = all_data.groupby(['Symbol', 'Time'])['Volume']
    med = grp_vol.transform('median')
    cap = AVG_VOLUME_WINSOR_K * med
    to_clip = (med > 0) & (all_data['Volume'] > cap)
    n_clipped = int(to_clip.sum())
    if n_clipped:
        # Log how much clipping actually happened so operators can spot
        # a symbol/session that would otherwise silently poison the
        # baseline. Info level so it's visible in a normal warmup log
        # without turning on debug.
        logger.info(
            "avg_volume winsorization: clipped %d/%d (Symbol,Time) rows "
            "at k=%.1f x median",
            n_clipped, len(all_data), AVG_VOLUME_WINSOR_K,
        )
        all_data.loc[to_clip, 'Volume'] = cap[to_clip]

    # Group by Symbol and Time, compute the mean of the (now winsorized)
    # per-slot volumes.
    avg_volume_df = (
        all_data.groupby(['Symbol', 'Time'], as_index=False)['Volume']
        .mean()
        .rename(columns={'Volume': 'Avg_volume'})
    )

    return avg_volume_df

def calculate_rvol(df: pd.DataFrame) -> pd.DataFrame:

    # Check required columns
    required_cols = ['Volume', 'Avg_volume']
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    # cumulative sums
    df['CumVolume'] = df['Volume'].cumsum()
    df['CumAvgVolume'] = df['Avg_volume'].cumsum()

    # Rvol = cumulative volume / cumulative avg volume
    df['Rvol'] = np.where(
        (df['CumAvgVolume'] == 0) | df['CumAvgVolume'].isna(),
        0.0,
        df['CumVolume'] / df['CumAvgVolume']
    )

    return df


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


def calculate_next_day_atr_ext(
    candle: CandleRow, atr_value: float, prev_close: float,
) -> CandleRow:
    """
    Live counterpart to ``calculate_day_atr_ext``. Same formula and
    sign convention: positive => close is below yesterday's close.
    """
    try:
        candle.day_atr_ext = round((prev_close - candle.close) / atr_value, 2)

    except Exception as e:
        logging.exception("Error calculating DayAtrExt for %s: %s", candle.symbol, e)
        candle.day_atr_ext = 0.0

    return candle


def calculate_next_rvol(candle: CandleRow, historical_df: pd.DataFrame, avg_volume: float) -> CandleRow:
    try:
        if 'Volume' not in historical_df.columns or 'Avg_volume' not in historical_df.columns:
            candle.rvol = 0.0
            return candle

        cum_avg_vol = float(historical_df['Avg_volume'].sum())
        cum_vol = float(historical_df['Volume'].sum())

        # include the new candle
        next_cum_vol = cum_vol + candle.volume
        next_cum_avg_vol = cum_avg_vol + avg_volume

        # safety
        if next_cum_avg_vol <= 0:
            candle.rvol = 0.0
        else:
            candle.rvol = round(next_cum_vol / next_cum_avg_vol, 4)

    except Exception as e:
        logging.exception("Error calculating Rvol for %s: %s", candle.symbol, e)
        candle.rvol = 0.0

    return candle




# Laskee positio koon kun tiedetaan nama
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
