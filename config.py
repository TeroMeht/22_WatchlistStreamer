# config.py
from pathlib import Path
from datetime import time

CLIENT_CONFIG = {
    "clientId": 0,
    "host": str("127.0.0.1"),
    "port": 4002,
    "port2": 7497,
    "capitulation_threshold": 0.45,
    "vwap_distance": 0.1,
    "extreme_extension_threshold": 3,
    "BOT_TOKEN": "7824990107:AAFwXNYNgjSC3VP8jFzbcE16_Hp19qmQx-4",
    "CHAT_ID": "7892660893",
    "tickers_folder": str(Path("./tickers")),
    "timezone": str("Europe/Helsinki"),
    "barbuffer_size": 1000,
    "alarm_cutoff_minutes": 10,
    "endofday": time(22,50),
    "exit-request_endpoint": str("http://127.0.0.1:8000/api/portfolio/exit-request"),
    "alarms_endpoint": str("http://127.0.0.1:8000/api/alarms/emit")
}