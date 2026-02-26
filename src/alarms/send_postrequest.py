import httpx
import logging
from src.helpers.handle_candles import CandleRow
from datetime import date,time

logger = logging.getLogger(__name__)

async def send_exit_request_to_fastapi(candle: CandleRow, alarm_name: str,fastapi_url:str):
    """
    Sends an alarm to the Flask /api/portfoliomanager endpoint using date and time from CandleRow.
    Converts date/time to string to be JSON serializable.
    """
    # Convert date/time to strings
    date_str = candle.date.isoformat() if hasattr(candle, "date") else str(candle.date)
    time_str = candle.time.isoformat() if hasattr(candle, "time") else str(candle.time)

    # Payload matching your ExitRequest model
    payload = {
        "date": date_str,
        "time": time_str,
        "alarm": alarm_name,
        "symbol": candle.symbol
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(f"FastAPI response for {candle.symbol}: {json_response}")
            return json_response
    except Exception as e:
        logger.error(f"Failed to send alarm for {candle.symbol} to Fast: {e}")
        return None


async def send_alarm_to_fastapi(candle: CandleRow, alarm_message: str, fastapi_url: str):
    """
    Sends an alarm to FastAPI.
    
    :param candle: CandleRow object containing candle info
    :param alarm_message: The alarm message to send
    :param fastapi_url: URL of the FastAPI endpoint
    :return: JSON response from FastAPI or None if failed
    """
    # Prepare payload matching FastAPI schema
    payload = {
        "Id": 0,   # fallback 0 if id not present
        "Symbol": candle.symbol,
        "Time": candle.time.isoformat(),   # time -> ISO string
        "Alarm": alarm_message,
        "Date": candle.date.isoformat()    # date -> ISO string
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(fastapi_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(f"Sent alarm for {candle.symbol}: {json_response}")
            return json_response

    except Exception as e:
        logger.error(f"Failed to send alarm for {candle.symbol}: {e}")
        return None