from src.database.db_functions import *
from src.alarms.plotchart import plot_intraday_chart
from src.alarms.send_telegram import *

import plotly.io as pio
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

async def detect_vwap_setup(candle: CandleRow, database_config, project_config):
    """
    Trigger a VWAP continuation setup alarm using the last row of the provided DataFrame.
    No checks; assumes df has relevant data.
    """
    # Log contextual info
    logging.info(
        "Generating VWAP continuation alarm for candle:\n" +
        json.dumps({
            "Symbol": candle.symbol,
            "Date": str(candle.date),
            "Time": str(candle.time),
            "Close": candle.close,
            "Relatr": candle.relatR,
            "Rvol": candle.rvol,
            "VWAP": candle.vwap,
        }, indent=4)
    )

    # Trigger the alarm
    await generate_signal_alarm(candle=candle,
                                signal_name="VWAP continuation setup",
                                database_config=database_config,
                                project_config=project_config
                            )








def is_crossover_up(df, price_col="Close", ema_col="EMA9"):
    if df is None or len(df) < 2:
        return False

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    crossed_from_below = prev[price_col] < curr[ema_col]
    closed_above_ema = curr[price_col] > curr[ema_col]

    return crossed_from_below and closed_above_ema

async def detect_ema_crossover_up(df, candle: CandleRow, database_config, project_config):
    """
    Detect upward EMA crossover and trigger alarm if detected.
    """
    try:
        if is_crossover_up(df.tail(2)):
            last_row = df.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            await generate_signal_alarm(candle=candle,
                                        signal_name="EMA9 crossover up",
                                        database_config=database_config,
                                        project_config=project_config
                                    )




        else:
            logging.info(f"{candle.symbol}: No EMA9 crossover up detected.\n")

    except Exception as e:
        logging.error(f"Error in detect_ema_crossover_up_from_df for {candle.symbol}: {e}")





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
                database_config=database_config,
                project_config=project_config
            )
        else:
            logging.info(f"{table_name}: No EMA9 crossover down detected.\n")

    except Exception as e:
        logging.error(f"Error in detect_ema_crossover_down_from_df for {table_name}: {e}")
        return False




# Generate alarm message and insert
async def generate_signal_alarm(candle: CandleRow,
                                signal_name: str,
                                database_config: dict,
                                project_config: dict):
    """
    Builds and sends an EMA9 crossover alarm if no recent duplicate exists.
  
      """
    try:
        # Check if a recent alarm already exists
        if not await alarm_exists_recently(symbol=candle.symbol,
                                            time_obj=candle.time,
                                            date_obj=candle.date,
                                            database_config=database_config,
                                            cutoff_minutes=project_config["alarm_cutoff_minutes"]):

            # Build alarm message
            alarm_msg = f"{signal_name} detected"

            # Insert alarm into DB
            await insert_alarm(
                symbol=candle.symbol,
                time_obj=candle.time,
                alarm_message=alarm_msg,
                date_obj=candle.date,
                database_config=database_config
            )
            # Send Telegram message
            # await send_telegram_message(
            #     symbol=candle.symbol,
            #     time_obj=candle.time,
            #     alarm_message=alarm_msg,
            #     bot_token=project_config["BOT_TOKEN"],
            #     chat_id=project_config["CHAT_ID"]
            # )

            intraday_data = await get_last_rows(table_name=candle.symbol.lower(), num_rows=None, database_config=database_config)
            # Create and show plot
            fig = plot_intraday_chart(intraday_data)
            # Convert figure to image in-memory (byte object)
            image_bytes = pio.to_image(fig, format='png')  # Convert to PNG in memory
            alarm_message = alarm_msg
            await send_telegram_picture(project_config, image_bytes, alarm_message)  # Send directly to Telegram

            logging.info(f"{candle.symbol}: Signal alarm '{signal_name}' sent successfully.")

    except Exception as e:
        logging.error(f"Error generating signal alarm for {candle.symbol}: {e}")
                                    
            


