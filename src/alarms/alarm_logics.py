from src.database.db_functions import *

from src.common.calculate import calculate_position_size

from src.alarms.send_telegram import *
import json
import logging

logger = logging.getLogger(__name__)  # module-specific logger

# Kun EMA9 crossover tapahtuu tunnista riskitaso johon stoppi tulee
async def detect_stoplevel(table_name, num_rows, database_config):

    df = await get_last_rows(table_name, num_rows, database_config)
    lowest_price = df["Low"].min()  # Use column index 6 if that's the 'Low' column

    return lowest_price


def detect_capitulation(df, threshold):

    try:
        if df is None or df.empty:
            return False

        # Vectorized check: select all rows exceeding the threshold
        capitulated_rows = df[df["Relatr"] >= threshold]

        if not capitulated_rows.empty:
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

def detect_euforia(df, threshold):
    """
    Detect euphoria: opposite of capitulation.
    Triggered when 'Relatr' is below -threshold (strong upward move).
    """
    try:
        if df is None or df.empty:
            return False

        # Vectorized check: select all rows below negative threshold
        euforia_rows = df[df["Relatr"] <= -threshold]

        if not euforia_rows.empty:
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




def is_vwap_close(df, vwap_distance, price_col="Relatr"):
    """
    Check if the last row's Relatr is within ±vwap_distance.
    """
    if df is None or df.empty:
        return False

    last_row = df.iloc[-1]
    relatr = last_row[price_col]

    return -vwap_distance <= relatr <= vwap_distance

async def detect_vwap_setup(df, table_name,database_config, project_config):
    """
    Trigger a VWAP continuation setup alarm using the last row of the provided DataFrame.
    No checks; assumes df has relevant data.
    """
    try:
        if df is None or df.empty:
            logging.info(f"{table_name}: No historical data provided.")
            return False

        # Use the last row in the DataFrame
        last_row = df.iloc[-1]

        selected = {
            "Symbol": last_row["Symbol"],
            "Time": last_row["Time"],
            "Date": last_row["Date"],
            "Relatr": last_row.get("Relatr", None),
        }

        logging.info("Generating VWAP continuation alarm for:\n" +
                     json.dumps(selected, indent=4, default=str))

        # Trigger the alarm
        await generate_signal_alarm(
            symbol=last_row["Symbol"],
            time_obj=last_row["Time"],
            date_obj=last_row["Date"],
            signal_name="VWAP continuation setup",
            close_price=last_row["Close"],
            table_name=table_name,
            database_config=database_config,
            project_config=project_config
        )

        return True

    except Exception as e:
        logging.error(f"Error generating VWAP setup alarm for {table_name}: {e}")
        return False









def is_crossover_up(df, price_col="Close", ema_col="EMA9"):
    if df is None or len(df) < 2:
        return False

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    crossed_from_below = prev[price_col] < curr[ema_col]
    closed_above_ema = curr[price_col] > curr[ema_col]

    return crossed_from_below and closed_above_ema

async def detect_ema_crossover_up(df, table_name, database_config, project_config):
    """
    Detect upward EMA crossover and trigger alarm if detected.
    """
    try:
        if is_crossover_up(df.tail(2)):
            last_row = df.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            await generate_signal_alarm(
                symbol=last_row["Symbol"],
                time_obj=last_row["Time"],
                date_obj=last_row["Date"],
                signal_name="EMA9 crossover up",
                close_price=last_row["Close"],
                table_name=table_name,
                database_config=database_config,
                project_config=project_config
            )
        else:
            logging.info(f"{table_name}: No EMA9 crossover up detected.\n")

    except Exception as e:
        logging.error(f"Error in detect_ema_crossover_up_from_df for {table_name}: {e}")





def is_crossover_down(df, price_col="Close", ema_col="EMA9"):
    """
    Check if the price crossed EMA9 from above to below.
    """
    if df is None or len(df) < 2:
        return False

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    crossed_from_above = prev[price_col] > prev[ema_col]
    closed_below_ema = curr[price_col] < curr[ema_col]

    return crossed_from_above and closed_below_ema

async def detect_ema_crossover_down(df, table_name, database_config, project_config):
    """
    Detect downward EMA crossover and trigger alarm if detected.
    """
    try:
        if is_crossover_down(df.tail(2)):
            last_row = df.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover down detected, generating alarm...")

            await generate_signal_alarm(
                symbol=last_row["Symbol"],
                time_obj=last_row["Time"],
                date_obj=last_row["Date"],
                signal_name="EMA9 crossover down",
                close_price=last_row["Close"],
                table_name=table_name,
                database_config=database_config,
                project_config=project_config
            )
        else:
            logging.info(f"{table_name}: No EMA9 crossover down detected.\n")

    except Exception as e:
        logging.error(f"Error in detect_ema_crossover_down_from_df for {table_name}: {e}")
        return False




# Generate alarm message and insert
async def generate_signal_alarm(
    symbol, time_obj, date_obj, signal_name, close_price,
    table_name, database_config, project_config, stop_price=None
):
    """
    Builds and sends an EMA9 crossover alarm if no recent duplicate exists.
  
      """
    try:
        if not await alarm_exists_recently(symbol, time_obj, date_obj, database_config, cutoff_minutes=15):
        
          #  stop_price = await detect_stoplevel(table_name, num_rows=5, database_config=database_config)
          #  position_size = calculate_position_size(close_price, stop_price, risk=project_config["risk"])

            # Build alarm message
            alarm_msg = f"{signal_name} detected for {symbol} at {close_price:.2f}"

            # Insert alarm and send Telegram message
            await insert_alarm(symbol, time_obj, alarm_msg, date_obj, database_config)
            await send_telegram_message(
                symbol, time_obj, alarm_msg,
                bot_token=project_config["BOT_TOKEN"],
                chat_id=project_config["CHAT_ID"]
            )

            logging.info(f"{symbol}: Signal alarm '{signal_name}' sent successfully.")

    except Exception as e:
        logging.error(f"Error generating signal alarm for {symbol}: {e}")