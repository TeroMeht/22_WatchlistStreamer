from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Optional, List,Dict

def get_2min_interval(dt: datetime) -> datetime:
    """Round datetime down to nearest 2-minute interval."""
    minute = (dt.minute // 2) * 2
    return dt.replace(second=0, microsecond=0, minute=minute)



def validate_datasets(df_list:List[pd.DataFrame], tickers:list, dataset_name:str) -> set:

    found = set()

    for df in df_list:
        if df is not None and hasattr(df, "empty") and not df.empty:
            col = "Symbol" 
            if col:
                found.update(df[col].unique())

    missing = [t for t in tickers if t not in found]

    if missing:
        logging.warning(f"{dataset_name}: Missing symbols {missing}")

    return found or set()   #  ensures it never returns None


def detect_stoplevel(df: pd.DataFrame, direction: str, offset: float = 0.06) -> float:
    """
    Calculate stop level based on recent High/Low in DataFrame.

    :param df: DataFrame with at least 'High' and 'Low' columns
    :param direction: 'long' or 'short'
        - 'long' -> stop below recent low
        - 'short' -> stop above recent high
    :param offset: distance from reference price (default 0.02)
    :return: stop level price
    """

    if df.empty:
        raise ValueError("DataFrame is empty, cannot detect stop level")

    required_cols = {"High", "Low"}
    if not required_cols.issubset(df.columns):
        raise KeyError(f"DataFrame must contain columns: {required_cols}")

    direction = direction.lower()

    if direction == "long":
        reference_price = df["Low"].min()
        stop_level = round(reference_price - offset, 2)
    elif direction == "short":
        reference_price = df["High"].max()
        stop_level = round(reference_price + offset, 2)
    else:
        raise ValueError(f"Unsupported direction: {direction}")

    return stop_level
