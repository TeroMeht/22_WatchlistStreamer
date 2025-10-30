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



def intraday_datapipe(bars: List[IncomingBar],
                      time_zone:str) -> pd.DataFrame:
    """
    Convert a list of IncomingBar dataclasses to a pandas DataFrame.
    Keeps datetime as timezone-aware and capitalizes all column names.
    """

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


def handle_incoming_dataframe_intraday(bars: List[IncomingBar], 
                                       symbol:str,
                                       time_zone:str)-> pd.DataFrame:
    """
    Process IBKR bars into a pandas DataFrame (or dataclasses if needed):
    - Adjust timezone
    - Calculate VWAP / EMA9
    """
    # Step 1: Convert to dataclasses
    incoming_bars = incoming_bars_to_datamodel_format(bars)

    # Step 2: Convert to DataFrame
    df = intraday_datapipe(incoming_bars,time_zone)

    # Step 4: Assign symbol
    df["Symbol"] = symbol

    # Step 5: Calculate indicators
    df = calculate_vwap(df)
    df = calculate_ema(df, period=9)


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

    logger.info(df.tail(10))
    return df




def build_last_atr_dict(daily_results_with_atr: List[pd.DataFrame]) -> Dict[str, float]:
    """
    Build a dictionary mapping each symbol to its last ATR value from daily DataFrames.
    
    Assumes each DataFrame contains columns 'Symbol' and 'ATR', and has only one symbol per DataFrame.
    """
    return {df['Symbol'].iloc[0]: df['ATR'].iloc[-1] for df in daily_results_with_atr}

def handle_Atr_intraday_dataset(intraday_results: list[pd.DataFrame],
                                daily_results_with_atr: list[pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], dict[str, float]]:

    relatr_datasets = {}
    last_atr_per_symbol = build_last_atr_dict(daily_results_with_atr)
    
    cols_order = ['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 
                  'Close', 'Volume', 'VWAP', 'EMA9', 'Relatr']

    for intraday_df in intraday_results:
        if intraday_df is None or intraday_df.empty:
            logger.warning("Empty intraday DataFrame, skipping.")
            continue
        
        symbol = intraday_df['Symbol'].iloc[0]
        intraday_df = calculate_relatr(intraday_df, last_atr_per_symbol)
        intraday_df = intraday_df[cols_order]
        relatr_datasets[symbol] = intraday_df
        logger.info(f"{symbol} - last 10 rows:\n{intraday_df.tail(10)}")

    return relatr_datasets, last_atr_per_symbol





