from src.database.db_functions import *

import json
import logging

logger = logging.getLogger(__name__)  # module-specific logger


def detect_capitulation(df:pd.DataFrame, threshold:float)-> bool:

    try:
        # Vectorized check: select all rows exceeding the threshold
        capitulated_rows = df[df["Relatr"] >= threshold]
        # No rows met the condition
        if capitulated_rows.empty:
            logging.debug("No capitulation rows found (Relatr >= %.3f).", threshold)
            return False
        # Take the last row that triggered capitulation
        last_row = capitulated_rows.iloc[-1]

        selected = {
            "Symbol": last_row["Symbol"],
            "Time": last_row["Time"],
            "Relatr": last_row["Relatr"],
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
    Triggered when 'Relatr' is below -threshold (strong upward move).
    """
    try:
        # Vectorized check: select all rows below negative threshold
        euforia_rows = df[df["Relatr"] <= -threshold]
        # No rows met the condition
        if euforia_rows.empty:
            logging.debug("No euforia rows found (Relatr >= %.3f).", threshold)
            return False
        # Take the last row that triggered euphoria
        last_row = euforia_rows.iloc[-1]

        selected = {
            "Symbol": last_row["Symbol"],
            "Time": last_row["Time"],
            "Relatr": last_row["Relatr"],
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
    return -vwap_distance <= candle.relatR <= vwap_distance

def is_crossover_up(df:pd.DataFrame)-> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_below = prev_row["Close"] < curr_row["EMA9"]
    closed_above_ema = curr_row["Close"] > curr_row["EMA9"]

    return crossed_from_below and closed_above_ema

def is_crossover_down(df:pd.DataFrame)-> bool:

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    crossed_from_above = prev_row["Close"] > prev_row["EMA9"]
    closed_below_ema = curr_row["Close"] < curr_row["EMA9"]

    return crossed_from_above and closed_below_ema








