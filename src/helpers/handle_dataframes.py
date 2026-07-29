from src.common.calculate import *
import pandas as pd
import logging
from typing import Optional, List,Dict
from src.database.db_functions import *


# Tämä on erillinen koodikirjasto jolla käsittelen sisään tulevia bars dataa pandas dataframeiksi
logger = logging.getLogger(__name__)  # module-specific logger

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from src.core.config import settings


@dataclass
class IncomingBar:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    average: Optional[float]
    barCount: Optional[int]


def incoming_bars_to_datamodel_format(bars) -> List[IncomingBar]:
    """
    Convert a list of raw IBKR bar objects into a list of IncomingBar dataclasses.
    
    Each bar must have attributes: date, open, high, low, close, volume
    Optional: average, barCount
    """
    incoming_bars = []
    
    for bar in bars:
        incoming_bars.append(
            IncomingBar(
                date=bar.date,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                average=getattr(bar, "average", None),
                barCount=getattr(bar, "barCount", None)
            )
        )

    return incoming_bars

def intraday_datapipe(bars: List[IncomingBar]) -> pd.DataFrame:
    """
    Convert a list of IncomingBar dataclasses to a pandas DataFrame.
    Keeps datetime as timezone-aware and capitalizes all column names.
    """
    time_zone = settings.TIMEZONE
    # Convert dataclasses to DataFrame
    df = pd.DataFrame([asdict(bar) for bar in bars])

    # Drop optional columns if present
    df = df.drop(columns=[c for c in ["average", "barCount"] if c in df.columns])

    # --- Convert datetime to Helsinki timezone ---
    df["date"] = pd.to_datetime(df["date"], utc=True)  # treat all as UTC
    df["date"] = df["date"].dt.tz_convert(ZoneInfo(time_zone))  # convert to Helsinki coming from project config

    # --- Split Date / Time for readability ---
    df["Date"] = df["date"].dt.date      # keep only date part
    df["Time"] = df["date"].dt.time      # optional: separate Time column

    # Drop original 'date' column if you want
    df = df.drop(columns=["date"])

    # Capitalize all remaining column names
    df.columns = [col.capitalize() for col in df.columns]

    return df

def daily_datapipe(bars: List[IncomingBar]) -> pd.DataFrame:
    """
    Convert a list of IncomingBar dataclasses to a pandas DataFrame.
    Keeps datetime as timezone-aware and capitalizes all column names.
    """

    # Convert dataclasses to DataFrame
    df = pd.DataFrame([asdict(bar) for bar in bars])

    # Capitalize all remaining column names
    df.columns = [col.capitalize() for col in df.columns]

    return df




def handle_incoming_dataframe_daily(bars: List[IncomingBar], symbol:str)-> pd.DataFrame:


    incoming_bars = incoming_bars_to_datamodel_format(bars)

    df = daily_datapipe(incoming_bars)
    
    df['Symbol'] = symbol


    # Calculate ATR (assumes this function adds Prev_Close, TR, ATR columns)
    df = calculate_14day_atr_df(df)

    # --- Reorder columns ---
    desired_order = [
        "Symbol","Date", "Open", "High", "Low", "Close", "Volume",
        "Average", "BarCount", "Prev_Close", "TR", "ATR"
    ]
    # Keep only columns that exist (some may be missing)
    df = df[[col for col in desired_order if col in df.columns]]


    return df

def handle_incoming_dataframe_intradays_volume(bars: List[IncomingBar], symbol:str)-> pd.DataFrame:

    # Step 1: Convert to dataclasses
    incoming_bars = incoming_bars_to_datamodel_format(bars)

    # Step 2: Convert to DataFrame
    df = intraday_datapipe(incoming_bars)

    # Step 4: Assign symbol
    df["Symbol"] = symbol
            # Step 4: Ensure Date column is datetime
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # Step 5: Split today vs past
    today = date.today()

    df_today = df[df["Date"] == today].copy()
    df_past = df[df["Date"] != today].copy()
    # Keep only rows with time >= 11:00
    df_today = df_today[df_today["Time"] >= time(11, 0)]


    df_past = df_past[["Symbol","Date","Time","Open","High","Low","Close","Volume"]]

    # Step 5: Calculate average volume model
    df_past = calculate_avg_volume_model([df_past])
    df_today = calculate_vwap(df_today)
    df_today = calculate_ema(df_today, period=9)

    df_today = df_today[[
        "Symbol","Date","Time","Open","High","Low","Close","Volume","VWAP","EMA9"
    ]]
    return df_today,df_past



def build_last_atr_dict(daily_with_atr: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """Latest ATR per symbol from the daily-bars dict keyed by Symbol."""
    return {symbol: df['ATR'].iloc[-1] for symbol, df in daily_with_atr.items()}

def handle_Atr_intraday_dataset(
    intraday_results: dict[str, pd.DataFrame],
    daily_with_atr: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict[str, float]]:

    relatr_datasets = {}

    # Build last ATR dictionary
    last_atr_per_symbol = build_last_atr_dict(daily_with_atr)

    # Desired column order
    cols_order = [
        'Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close',
        'Volume', 'VWAP', 'EMA9', 'Avg_volume', 'Rvol', 'Relatr'
    ]

    # Upstream validate_tickers guarantees every frame here is non-empty.
    for symbol, intraday_df in intraday_results.items():
        # Calculate Relatr using the existing intraday DataFrame
        intraday_df = calculate_relatr(intraday_df, last_atr_per_symbol)

        # Reorder columns
        intraday_df = intraday_df[cols_order]

        relatr_datasets[symbol] = intraday_df
        logger.debug(f"{symbol} - last 10 rows:\n{intraday_df.tail(10)}")

    return relatr_datasets, last_atr_per_symbol

def handle_intraday_rvol_dataset(
    today_intra: dict[str, pd.DataFrame],
    past_intra:  dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Join today's 2-min intraday frame against the 5-day average-volume
    frame per symbol and compute Rvol. Both inputs are dicts keyed by
    Symbol -- upstream ``validate_tickers`` guarantees the same key set
    in both, so we just iterate one of them.
    """
    required_cols = ['Symbol', 'Time', 'Avg_volume']
    rvol_datasets = {}

    for symbol, intraday_df in today_intra.items():
        avg_volume_df = past_intra[symbol]
        missing = [c for c in required_cols if c not in avg_volume_df.columns]
        if missing:
            logger.error(f"Avg volume DataFrame for {symbol} missing columns: {missing}")
            continue

        merged_df = pd.merge(
            intraday_df,
            avg_volume_df[required_cols],
            on=['Symbol', 'Time'],
            how='left',
        )
        merged_df = calculate_rvol(merged_df)

        rvol_datasets[symbol] = merged_df
        logger.debug(f"{symbol} - last 10 rows with Rvol:\n{merged_df.tail(10)}")

    return rvol_datasets


