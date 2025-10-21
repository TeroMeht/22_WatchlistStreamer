from datetime import datetime, timedelta
import pandas as pd

def build_last_atr_dict(symbols, daily_results):
    """
    Build dictionary of the last ATR14 value per symbol.
    Used for passing into the live stream calculation.
    """
    last_atr_dict = {}

    for i, symbol in enumerate(symbols):
        daily_df = daily_results[i]
        if daily_df is not None and 'ATR' in daily_df.columns:
            last_atr = daily_df['ATR'].iloc[-1]
            last_atr_dict[symbol] = float(last_atr) if pd.notna(last_atr) else None
        else:
            last_atr_dict[symbol] = None  # fallback if missing data

    return last_atr_dict

def get_2min_interval(dt: datetime) -> datetime:
    """Round datetime down to nearest 2-minute interval."""
    minute = (dt.minute // 2) * 2
    return dt.replace(second=0, microsecond=0, minute=minute)