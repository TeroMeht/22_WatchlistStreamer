from src.database.db_functions import *

import logging

logger = logging.getLogger(__name__)  # module-specific logger


# NOTE: ``detect_capitulation`` and ``detect_euforia`` used to live here
# but were only ever consumed by the reversal strategies + the two
# momentum exit strategies (both regime-based). They now live at
# ``src.strategies.reversal_shared.detection`` next to the reversal
# code that owns the concept. Import them from there.


def is_vwap_close(candle: CandleRow, vwap_distance: float) -> bool:
    # Palauttaa true jos viimeisimmän sisään tulleen candle relatr on määritellyn vwap_distance sisällä
    return -vwap_distance <= candle.relatr <= vwap_distance


def is_crossover_up(df: pd.DataFrame) -> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_below = prev_row["close"] < curr_row["ema9"]
    closed_above_ema = curr_row["close"] > curr_row["ema9"]

    return crossed_from_below and closed_above_ema


def is_crossover_down(df: pd.DataFrame) -> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_above = prev_row["close"] > prev_row["ema9"]
    closed_below_ema = curr_row["close"] < curr_row["ema9"]

    return crossed_from_above and closed_below_ema
