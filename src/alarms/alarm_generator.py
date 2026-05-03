from src.database.db_functions import *
from src.alarms.alarm_plotchart import plot_intraday_chart
from src.alarms.send_telegram import *
from src.alarms.send_postrequest import *

import plotly.io as pio
import logging

from src.core.config import settings


logger = logging.getLogger(__name__)  # module-specific logger


# Generate alarm message and insert
async def generate_signal_alarm(candle: CandleRow, signal_name: str)-> None:
    """
    Builds and sends an EMA9 crossover alarm if no recent duplicate exists.
  
      """
    try:
      
        # Check if a recent alarm already exists
        if not await alarm_exists_recently(candle=candle,
                                           alarm_message=signal_name,                             
                                            cutoff_minutes=settings.ALARM_CUTOFF_MINUTES):

            # Insert alarm into DB
            await insert_alarm(candle=candle,
                                alarm_message=signal_name)
            

             # Send alarm to FastAPI asynchronously
            await send_alarm_to_fastapi(candle=candle, alarm_message=signal_name, fastapi_url=settings.ALARMS_ENDPOINT)


            #intraday_data = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None)
            # Create and show plot

            # Send photo
           # fig = plot_intraday_chart(intraday_data)

           # image_bytes = await asyncio.to_thread(pio.to_image, fig, format='png')
           # await send_telegram_picture(image_bytes, alarm_message)  # Send directly to Telegram
            
            # # Send ONLY message (no picture)
            await send_telegram_message(
                symbol=candle.symbol,
                time_obj=candle.time,
                alarm_message=signal_name)

            logging.info("%s: Signal alarm '%s' sent successfully.",candle.symbol,signal_name)


    except Exception as e:
        logging.error(f"Error generating signal alarm for {candle.symbol}: {e}")
