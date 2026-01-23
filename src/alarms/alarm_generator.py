from src.database.db_functions import *
from src.alarms.alarm_plotchart import plot_intraday_chart
from src.alarms.send_telegram import *



import plotly.io as pio
import logging

logger = logging.getLogger(__name__)  # module-specific logger


# Generate alarm message and insert
async def generate_signal_alarm(candle: CandleRow,
                                signal_name: str,
                                stop_level:float,
                                project_config: dict)-> None:
    """
    Builds and sends an EMA9 crossover alarm if no recent duplicate exists.
  
      """
    try:
      
        # Check if a recent alarm already exists
        if not await alarm_exists_recently(candle=candle,
                                           alarm_message=signal_name,                             
                                            cutoff_minutes=project_config["alarm_cutoff_minutes"]):

            # Insert alarm into DB
            await insert_alarm(candle=candle,
                                alarm_message=signal_name)
            # Insert active order to DB
            await insert_order(candle=candle,
                               stop_level=stop_level)

            intraday_data = await get_last_rows(table_name=candle.symbol.lower(), num_rows=None)
            # Create and show plot

            fig = plot_intraday_chart(intraday_data)
            # Convert figure to image in-memory (byte object)
            image_bytes = pio.to_image(fig, format='png')  # Convert to PNG in memory

            alarm_message = f"{candle.symbol}: {signal_name} detected, Rvol: {candle.rvol}:"
            await send_telegram_picture(project_config, image_bytes, alarm_message)  # Send directly to Telegram

            logging.info(f"{candle.symbol}: Signal alarm '{signal_name}' sent successfully.")

    except Exception as e:
        logging.error(f"Error generating signal alarm for {candle.symbol}: {e}")
                                    
            