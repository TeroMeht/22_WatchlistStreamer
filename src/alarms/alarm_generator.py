from src.database.db_functions import *
from src.alarms.alarm_plotchart import plot_intraday_chart
from src.alarms.send_telegram import *
from src.alarms.send_postrequest import *



import asyncio
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

            # Only fetch the most recent candles needed for the intraday chart.
            # Previously this used num_rows=None which pulled the entire symbol
            # table (potentially hundreds of rows) every time an alarm fired.
            intraday_data = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=100)
            # Create and show plot

            alarm_message = f"{candle.symbol}: {signal_name} detected, Rvol: {candle.rvol}:"

            # Send photo
            fig = plot_intraday_chart(intraday_data)
            # Convert figure to image in-memory (byte object).
            # pio.to_image is synchronous and CPU-heavy (it invokes a headless
            # browser via kaleido), which would otherwise block the asyncio
            # event loop for seconds while rendering. Offload it to a worker
            # thread so other concurrent tasks (real-time bar processing for
            # other tickers, DB inserts, etc.) keep running during the render.
            image_bytes = await asyncio.to_thread(pio.to_image, fig, format='png')
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
