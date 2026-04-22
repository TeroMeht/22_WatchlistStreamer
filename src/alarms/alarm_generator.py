from src.database.db_functions import *
from src.alarms.alarm_plotchart import plot_intraday_chart
from src.alarms.send_telegram import *
from src.alarms.send_postrequest import *



import plotly.io as pio
import logging

logger = logging.getLogger(__name__)  # module-specific logger


# Generate alarm message and insert
async def generate_signal_alarm(candle: CandleRow,
                                signal_name: str,
                                stop_level:float|None,
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
            
            if stop_level is not None:
                logger.info(f"Stop level for {candle.symbol} set at {stop_level}")
            # Insert active order to DB
            await insert_order(candle=candle,
                               stop_level=stop_level)
            
             # Send alarm to FastAPI asynchronously
            await send_alarm_to_fastapi(candle=candle, alarm_message=signal_name, fastapi_url=project_config["alarms_endpoint"])

            intraday_data = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None)
            # Create and show plot

            alarm_message = f"{candle.symbol}: {signal_name} detected, Rvol: {candle.rvol}:"

            # Send photo
            fig = plot_intraday_chart(intraday_data)
            # Convert figure to image in-memory (byte object)
            image_bytes = pio.to_image(fig, format='png')  # Convert to PNG in memory
            await send_telegram_picture(project_config, image_bytes, alarm_message)  # Send directly to Telegram
            
            # # Send ONLY message (no picture)
            # await send_telegram_message(
            #     symbol=candle.symbol,
            #     time_obj=candle.time,
            #     alarm_message=alarm_message,
            #     bot_token=project_config["BOT_TOKEN"],
            #     chat_id=project_config["CHAT_ID"],
            # )

            logging.info(
                "%s: Signal alarm '%s' sent successfully.",
                candle.symbol,
                signal_name
            )


    except Exception as e:
        logging.error(f"Error generating signal alarm for {candle.symbol}: {e}")
                                    
            