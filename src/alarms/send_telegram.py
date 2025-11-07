import requests

import logging

logger = logging.getLogger(__name__)  # module-specific logger




def format_telegram_message(symbol, time_obj, alarm_message):

    message = (
        f"🚨 Alarm triggered 🚨\n"
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
    Send a formatted Telegram message.
    This function now handles formatting internally.
    """
    message = format_telegram_message(symbol, time_obj, alarm_message)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, data=payload)
        result = response.json()
        if result.get("ok"):
            safe_print(f"Telegram message sent successfully: {result}")
        else:
            safe_print(f"Telegram API error: {result}")
        return result
    except Exception as e:
        safe_print(f"Error sending Telegram message: {e}, raw response: {response.text}")
        return {"ok": False, "error": str(e)}
    




async def send_telegram_picture(project_config: dict, image_bytes, alarm_message:str)-> dict:
    """
    Send a picture to a Telegram chat from a byte object (in-memory image),
    with optional caption text.
    
    :param bot_token: Telegram bot token
    :param chat_id: Chat ID where the image will be sent
    :param image_bytes: Image in byte format
    :param caption: Optional text caption to send with the image
    """
    telegram_endpoint = f"https://api.telegram.org/bot{project_config["BOT_TOKEN"]}/sendPhoto"
    
    try:
        # Prepare the payload for the request
        image_payload = {
            "chat_id": project_config["CHAT_ID"],
            "caption": alarm_message  # Add caption if provided
        }

        # Send the image in-memory (as bytes)
        files = {
            'photo': ('image.png', image_bytes, 'image/png')
        }

        # Send the request
        response = requests.post(telegram_endpoint, data=image_payload, files=files)
        result = response.json()

        if result.get("ok"):
            logger.info(f"Image sent successfully: {result}")
        else:
            logger.error(f"Error sending image: {result}")

        return result

    except Exception as e:
        logger.error(f"Error sending image to Telegram: {e}")
        return {"ok": False, "error": str(e)}
