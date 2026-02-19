# config.py
from pathlib import Path
from datetime import time

CLIENT_CONFIG = {
    "clientId": 0,
    "host": str("127.0.0.1"),
    "port2": 4002,
    "port": 7497,
    "capitulation_threshold": 0.45,
    "vwap_distance": 0.1,
    "extreme_extension_threshold": 2,
    "BOT_TOKEN": "7824990107:AAFwXNYNgjSC3VP8jFzbcE16_Hp19qmQx-4",
    "CHAT_ID": "7892660893",
    "tickers_folder": str(Path("./tickers")),
    "timezone": str("Europe/Helsinki"),
    "barbuffer_size": 1000,
    "alarm_cutoff_minutes": 10,
    "endofday": time(22,50),
    "flask_url": str("http://127.0.0.1:8080/api/portfoliomanager"),
    "livestream_endpoint": str("http://127.0.0.1:8080/api/livestream/emit")
}