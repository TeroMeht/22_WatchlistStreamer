import httpx

import logging

logger = logging.getLogger(__name__)  # module-specific logger


# Default timeout for Telegram HTTP calls (seconds)
_TELEGRAM_TIMEOUT = 10.0




def format_telegram_message(symbol, time_obj, alarm_message):

    message = (
        f"\U0001F6A8 Alarm triggered \U0001F6A8\n"
        f"Symbol: {symbol}\n"
        f"Time: {time_obj}\n"
        f"Message: {alarm_message}"
    )
    return message


def safe_print(*args, **kwargs):
    """Safe print that strips/ignores characters console can't handle."""
    try:
        logging.info(*args, **kwargs)
    except UnicodeEncodeError:
        msg = " ".join(str(a) for a in args)
        logging.info(msg.encode("ascii", errors="ignore").decode(), **kwargs)


async def send_telegram_message(symbol, time_obj, alarm_message, bot_token, chat_id):
    """
    Send a formatted Telegram message asynchronously.
    Uses httpx.AsyncClient so the event loop is not blocked while
    waiting for the Telegram API response.
    """
    message = format_telegram_message(symbol, time_obj, alarm_message)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        async with httpx.AsyncClient(timeout=_TELEGRAM_TIMEOUT) as client:
            response = await client.post(url, data=payload)
        result = response.json()
        if result.get("ok"):
            safe_print(f"Telegram message sent successfully: {result}")
        else:
            safe_print(f"Telegram API error: {result}")
        return result
    except Exception as e:
        safe_print(f"Error sending Telegram message: {e}")
        return {"ok": False, "error": str(e)}




async def send_telegram_picture(project_config: dict, image_bytes, alarm_message: str) -> dict:
    """
    Send a picture to a Telegram chat from a byte object (in-memory image),
    with optional caption text. Non-blocking: uses httpx.AsyncClient so other
    async tasks (e.g. real-time bar processing) keep running while the upload
    is in flight.

    :param project_config: dict containing BOT_TOKEN and CHAT_ID
    :param image_bytes: Image in byte format
    :param alarm_message: Optional text caption to send with the image
    """
    bot_token = project_config["BOT_TOKEN"]
    chat_id = project_config["CHAT_ID"]
    telegram_endpoint = f"https://api.telegram.org/bot{bot_token}/sendPhoto"

    try:
        # Prepare the payload for the request
        image_payload = {
            "chat_id": chat_id,
            "caption": alarm_message  # Add caption if provided
        }

        # Send the image in-memory (as bytes)
        files = {
            'photo': ('image.png', image_bytes, 'image/png')
        }

        # Send the request asynchronously (does not block the event loop)
        async with httpx.AsyncClient(timeout=_TELEGRAM_TIMEOUT) as client:
            response = await client.post(telegram_endpoint, data=image_payload, files=files)
        result = response.json()

        if result.get("ok"):
            logger.info(f"Image sent successfully: {result}")
        else:
            logger.error(f"Error sending image: {result}")

        return result

    except Exception as e:
        logger.error(f"Error sending image to Telegram: {e}")
        return {"ok": False, "error": str(e)}
