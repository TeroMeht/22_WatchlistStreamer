from datetime import datetime, timedelta
import pandas as pd


def get_2min_interval(dt: datetime) -> datetime:
    """Round datetime down to nearest 2-minute interval."""
    minute = (dt.minute // 2) * 2
    return dt.replace(second=0, microsecond=0, minute=minute)



