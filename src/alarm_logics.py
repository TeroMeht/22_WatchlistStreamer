from src.database.db_functions import *

from src.common.calculate import calculate_position_size

from src.helpers.send_telegram import send_telegram_message
import json
from src.common.read_configs_in import read_database_config
import logging

logger = logging.getLogger(__name__)  # module-specific logger

# Kun EMA9 crossover tapahtuu tunnista riskitaso johon stoppi tulee
def detect_stoplevel(table_name, num_rows, database_config):

    df = get_last_rows(table_name, num_rows, database_config)
    lowest_price = df["Low"].min()  # Use column index 6 if that's the 'Low' column

    return lowest_price


def detect_capitulation(table_name, num_rows, database_config, threshold):
    try:
        df = get_last_rows(table_name, num_rows, database_config)

        # Loop through DataFrame rows
        for _, row in df.iterrows():
            relatr = row["Relatr"]


        if relatr >= threshold:  # kapitulaatio alaspäin
            # pick only the fields you care about
            selected = {
                "Symbol": row["Symbol"],
                "Time": row["Time"],
                "Relatr": row["Relatr"],
            }

            logging.info("Capitulation detected:\n" +
                json.dumps(selected, indent=4, default=str))

            return True

    except Exception as e:
        logging.error(f"Error in detect_capitulation: {e}")

    return False


def detect_euforia(table_name, num_rows, database_config, threshold):
    try:
        df = get_last_rows(table_name, num_rows, database_config)

        # Loop through DataFrame rows
        for _, row in df.iterrows():
            relatr = row["Relatr"]


        if relatr <= -threshold:  # kapitulaatio ylöspäin
            # pick only the fields you care about
            selected = {
                "Symbol": row["Symbol"],
                "Time": row["Time"],
                "Relatr": row["Relatr"],
            }

            logging.info("Euforia detected:\n" +
                json.dumps(selected, indent=4, default=str))

                # Return detection flag + time + date
            return True

    except Exception as e:
        logging.error(f"Error in detect_euforia: {e}")

    return False



def detect_ema9crossover_up(table_name, num_rows, database_config, project_config):
    """
    Detect EMA9 upward crossover and send alarm if triggered,
    but skip if a similar alarm exists recently.
    """
    try:
        df = get_last_rows(table_name, num_rows, database_config)
        if len(df) < 2:
            logging.warning("Not enough data to check EMA9 crossover.")
            return False

        first_row = df.iloc[0]
        second_row = df.iloc[1]

        first_close = first_row["Close"]
        second_close = second_row["Close"]
        second_ema9 = second_row["EMA9"]

        logging.info("Checking for Crossover: up")
        cond1 = first_close < second_ema9
        cond2 = second_close > second_ema9

        if cond1 and cond2:
            logging.info("ALARM: Entry EMA9 Crossover up")
            database_config1 = read_database_config(filename="database.ini", section="postgresql")
            symbol = second_row["Symbol"]
            time_obj = second_row["Time"]
            date_obj = second_row["Date"]

            # Build alarm text first (needed for check)
            stop_price = detect_stoplevel(table_name, num_rows=5, database_config=database_config)
            position_size = calculate_position_size(second_close, stop_price, risk=project_config["risk"])

            alarm_msg = (
                f"Entry ALARM: EMA9 crossover up  "
                f"Stop: {stop_price}  "
                f"Pos size: {position_size}"
            )

            # Check if similar alarm exists recently
            if alarm_exists_recently(symbol, time_obj, date_obj, database_config1, cutoff_minutes=15):
                logging.info("Skipping alarm — already exists recently.")
                return False

            # Insert and send
            insert_alarm(symbol, time_obj, alarm_msg, date_obj, database_config1)
            # Send Telegram message (formatting handled internally)
            send_telegram_message(
                symbol,
                time_obj,
                alarm_msg,
                bot_token=project_config["BOT_TOKEN"],
                chat_id=project_config["CHAT_ID"]
            )

            return True

    except Exception as e:
        logging.error(f"Error in detect_ema9crossover_up: {e}")

    return False


def detect_ema9crossover_down(table_name, num_rows, database_config, project_config):
    """
    Detect EMA9 downward crossover and send alarm if triggered,
    but skip if a similar alarm exists recently.
    """
    try:
        df = get_last_rows(table_name, num_rows, database_config)
        if len(df) < 2:
            logging.warning("Not enough data to check EMA9 crossover.")
            return False

        first_row = df.iloc[0]
        second_row = df.iloc[1]

        first_close = first_row["Close"]
        second_close = second_row["Close"]
        second_ema9 = second_row["EMA9"]

        logging.info("Checking for Crossover: down")
        cond1 = first_close > second_ema9
        cond2 = second_close < second_ema9

        if cond1 and cond2:
            logging.info("ALARM: Exit EMA9 Crossover down")
            database_config1 = read_database_config(filename="database.ini", section="postgresql")
            symbol = second_row["Symbol"]
            time_obj = second_row["Time"]
            date_obj = second_row["Date"]

            alarm_msg = "Exit ALARM: EMA9 crossover down"

            # Check if similar alarm exists recently
            if alarm_exists_recently(symbol, time_obj, date_obj, database_config1, cutoff_minutes=15):
                logging.info("Skipping alarm — already exists recently.")
                return False

            # Insert and send
            insert_alarm(symbol, time_obj, alarm_msg, date_obj, database_config1)
            # Send Telegram message (formatting handled internally)
            send_telegram_message(
                symbol,
                time_obj,
                alarm_msg,
                bot_token=project_config["BOT_TOKEN"],
                chat_id=project_config["CHAT_ID"]
            )

            return True

    except Exception as e:
        logging.error(f"Error in detect_ema9crossover_down: {e}")

    return False

def detect_euforia_all(table_name, database_config, threshold):

    try:
        # Fetch all rows
        df = fetch_historical_data(table_name, database_config)

        if df.empty:
            return False

        # Loop through DataFrame rows
        for _, row in df.iterrows():
            relatr = row["Relatr"]

            if relatr <= -threshold:  # kapitulaatio ylöspäin
                selected = {
                    "Symbol": row["Symbol"],
                    "Time": row["Time"],
                    "Relatr": relatr,
                }

                logging.info("Euforia detected:\n" +
                             json.dumps(selected, indent=4, default=str))

                # Return immediately on first detection
                return True

    except Exception as e:
        logging.error(f"Error in detect_euforia_all: {e}")

    return False







def detect_vwap_closeness(table_name, num_rows, database_config, vwap_distance, project_config):

    try:
        # Fetch the latest rows
        df = get_last_rows(table_name, num_rows, database_config)

        if df.empty:
            logging.info("No data returned from get_last_rows.")
            return False

        # Use the most recent row
        row = df.iloc[0]
        relatr = row["Relatr"]
        symbol = row["Symbol"]
        time_obj = row["Time"]
        date_obj = row["Date"]
        logging.info(f"[{symbol}] Relatr: {relatr:.5f} | VWAP distance threshold: ±{vwap_distance}")
        
        # Check if relatr is between -vwap_distance and +vwap_distance
        if -vwap_distance <= relatr <= vwap_distance:
            selected = {
                "Symbol": symbol,
                "Time": str(time_obj),
                "Date": str(date_obj),
                "Relatr": relatr,
            }
        
            logging.info("VWAP closeness detected:\n" +
                         json.dumps(selected, indent=4, default=str))

            # Reload DB config if alarms are stored separately
            database_config1 = read_database_config(filename="database.ini", section="postgresql")

            # Build alarm message
            alarm_msg = (
                f"VWAP closeness detected for {symbol} | "
                f"Relatr: {relatr:.4f} | Threshold: ±{vwap_distance}"
            )

            # Prevent duplicate alarms
            if alarm_exists_recently(symbol, time_obj, date_obj, database_config1, cutoff_minutes=15):
                logging.info("Skipping VWAP alarm — already exists recently.")
                return False

            # Insert alarm
            insert_alarm(symbol, time_obj, alarm_msg, date_obj, database_config1)

            # Send Telegram alert
            send_telegram_message(
                symbol,
                time_obj,
                alarm_msg,
                bot_token=project_config["BOT_TOKEN"],
                chat_id=project_config["CHAT_ID"]
            )

            logging.info(f"VWAP alarm sent for {symbol}")
            return True

        # Not within range
        return False

    except Exception as e:
        logging.error(f"Error in detect_vwap_closeness: {e}")
        return False
