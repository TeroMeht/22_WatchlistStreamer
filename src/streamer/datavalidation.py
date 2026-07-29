
import pandas as pd
import logging
from typing import List



# =============================================================================
# ticker validation
# =============================================================================


def validate_tickers(
    daily_data: list,
    today_intradaydata: list,
    past_intradaydata: list,
    tickers: list,
) -> list:
    """
    Keep only tickers present in ALL THREE historical datasets. Anything
    missing from one is unusable downstream (indicator calc joins across
    all three), so it gets dropped. Dropped tickers are logged once as a
    warning; the returned list preserves the input ordering.
    """
    found_intraday = validate_datasets(today_intradaydata, tickers, "2-min intraday")
    found_daily    = validate_datasets(daily_data,          tickers, "14-day daily")
    found_volume   = validate_datasets(past_intradaydata,   tickers, "5-day intraday")

    valid = [t for t in tickers if t in found_intraday and t in found_daily and t in found_volume]

    dropped = [t for t in tickers if t not in valid]
    if dropped:
        logging.warning(
            "Dropped %d tickers due to missing datasets: %s",
            len(dropped), ", ".join(dropped),
        )

    return valid



# =============================================================================
# dataset validation
# =============================================================================


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
