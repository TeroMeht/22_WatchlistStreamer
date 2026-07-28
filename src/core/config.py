from datetime import time
from pathlib import Path
from typing import Any
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    DATABASE_URL: str

    # --- Interactive Brokers ---
    IB_HOST: str
    IB_PORT: int
    IB_CLIENT_ID: int

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_CHAT_ID: str

    # --- Strategy parameters ---
    CAPITULATION_THRESHOLD: float
    EUFORIC_THRESHOLD: float
    VWAP_DISTANCE: float
    EXTREME_EXTENSION_THRESHOLD: float
    ALARM_CUTOFF_MINUTES: int
    ENDOFDAY: time
    # When the trading session starts. Used by strategies that need all rows
    # since session open (e.g. VWAP continuation). Override via env if needed.
    SESSION_START: time = time(16, 30)

    # ORB strategy parameters
    ORB_STOP_OFFSET: float
    RVOL_THRESHOLD: float

    TIMEZONE: str
    EXIT_REQUEST_ENDPOINT: str
    ALARMS_ENDPOINT: str
    STREAMER_START_ENDPOINT: str



    # --- Validators ---
    @field_validator("ENDOFDAY", "SESSION_START", mode="before")
    def parse_time_fields(cls, v: Any) -> Any:
        """Accept ``HH:MM`` in addition to ISO ``HH:MM:SS``."""
        if isinstance(v, str) and v.count(":") == 1:
            return f"{v}:00"
        return v

    @field_validator("TIMEZONE")
    def validate_timezone(cls, v: str) -> str:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"Unknown timezone: {v!r}") from e
        return v


    class Config:
        # Centralized env-file repository (shared across all projects).
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "22_WatchlistStreamer.env"  # centralized project configs
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
