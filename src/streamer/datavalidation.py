
import pandas as pd
import logging
from typing import List

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
