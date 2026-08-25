from src.database.db_functions import *

import json
import logging

logger = logging.getLogger(__name__)  # module-specific logger


def detect_capitulation(df:pd.DataFrame, threshold:float)-> bool:

    try:
        # Vectorized check: select all rows exceeding the threshold
        capitulated_rows = df[df["relatr"] >= threshold]
        # No rows met the condition
        if capitulated_rows.empty:
            logging.debug("No capitulation rows found (Relatr >= %.3f).", threshold)
            return False
        # Take the last row that triggered capitulation
        last_row = capitulated_rows.iloc[-1]

        selected = {
            "symbol": last_row["symbol"],
            "time": last_row["time"],
            "relatr": last_row["relatr"],
        }

        logging.info(
            "Capitulation detected:\n" + json.dumps(selected, indent=4, default=str)
        )
        return True

    except Exception as e:
        logging.error(f"Error in detect_capitulation: {e}")

    return False



def detect_euforia(df:pd.DataFrame, threshold:float)-> bool:
    """
    Detect euphoria: opposite of capitulation.
    Triggered when 'relatr' is below -threshold (strong upward move).
    """
    try:
        # Vectorized check: select all rows below negative threshold
        euforia_rows = df[df["relatr"] <= threshold]
        # No rows met the condition
        if euforia_rows.empty:
            logging.debug("No euforia rows found (Relatr >= %.3f).", threshold)
            return False
        # Take the last row that triggered euphoria
        last_row = euforia_rows.iloc[-1]

        selected = {
            "symbol": last_row["symbol"],
            "time": last_row["time"],
            "relatr": last_row["relatr"],
        }

        logging.info(
            "Euforia detected:\n" + json.dumps(selected, indent=4, default=str)
        )
        return True

    except Exception as e:
        logging.error(f"Error in detect_euforia: {e}")

    return False

def is_vwap_close(candle:CandleRow, vwap_distance:float) -> bool:
    # Palauttaa true jos viimeisimmän sisään tulleen candle relatr on määritellyn vwap_distance sisällä
    return -vwap_distance <= candle.relatr <= vwap_distance

def is_crossover_up(df:pd.DataFrame)-> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_below = prev_row["close"] < curr_row["ema9"]
    closed_above_ema = curr_row["close"] > curr_row["ema9"]

    return crossed_from_below and closed_above_ema

def is_crossover_down(df:pd.DataFrame)-> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_above = prev_row["close"] > prev_row["ema9"]
    closed_below_ema = curr_row["close"] < curr_row["ema9"]

    return crossed_from_above and closed_below_ema








