import httpx
import logging
from src.helpers.handle_candles import CandleRow

logger = logging.getLogger(__name__)

async def send_alarm_to_flask(
    candle: CandleRow, 
    alarm_name: str, 
    flask_url: str = "http://127.0.0.1:8080/api/portfoliomanager"
):
    """
    Sends an alarm to the Flask /api/portfoliomanager endpoint using date and time from CandleRow.
    Converts date/time to string to be JSON serializable.
    """
    # Convert date/time to strings
    date_str = candle.date.isoformat() if hasattr(candle, "date") else str(candle.date)
    time_str = candle.time.isoformat() if hasattr(candle, "time") else str(candle.time)

    payload = {
        "Symbol": candle.symbol,
        "Alarm": alarm_name,
        "Date": date_str,
        "Time": time_str
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(flask_url, json=payload, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            logger.info(f"Flask response for {candle.symbol}: {json_response}")
            return json_response
    except Exception as e:
        logger.error(f"Failed to send alarm for {candle.symbol} to Flask: {e}")
        return None
