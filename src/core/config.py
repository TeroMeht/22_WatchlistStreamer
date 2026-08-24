from datetime import time
from pathlib import Path
from typing import Any, Optional
from pydantic import field_validator
from pydantic_settings import BaseSettings

# IB connection fields (IB_HOST / IB_PORT / IB_CLIENT_ID /
# IB_CONNECT_TIMEOUT_S) come from this mixin -- declared in ONE place
# across every project that talks to IB. Env-key names unchanged, so
# the existing ``22_WatchlistStreamer.env`` file keeps working.
from data_sources.ib._config import IBSourceConfig


class Settings(IBSourceConfig, BaseSettings):

    DATABASE_URL: str

    # --- Runtime mode ---
    MODE: str

    # --- Replay-only settings (ignored in live mode) ---
    REPLAY_DATABASE_URL: str
    REPLAY_DATA_DIR: Path
    REPLAY_SPEED: float          # 0 = instant, 1.0 = realtime, N = Nx
    REPLAY_START_TIME: time

    # IB_HOST / IB_PORT / IB_CLIENT_ID inherited from IBSourceConfig above.

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

    # --- Warmup data source ---
    HISTORY_SOURCE: str

    POLYGON_API_KEY: str
    POLYGON_BASE_URL: str

    EXIT_REQUEST_ENDPOINT: str
    ALARMS_ENDPOINT: str
    STREAMER_START_ENDPOINT: str
    ENTRY_REQUEST_ENDPOINT: str


    # --- Validators ---
    @field_validator("ENDOFDAY", "SESSION_START", "REPLAY_START_TIME", mode="before")
    def parse_time_fields(cls, v: Any) -> Any:
        """Accept ``HH:MM`` in addition to ISO ``HH:MM:SS``. Passes
        through ``None`` / empty string unchanged so the Optional
        ``REPLAY_START_TIME`` can stay unset."""
        if v is None or v == "":
            return None
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

    @field_validator("MODE")
    def validate_mode(cls, v: str) -> str:
        v = (v or "live").lower()
        if v not in ("live", "replay"):
            raise ValueError(f"MODE must be 'live' or 'replay', got {v!r}")
        return v

    @field_validator("HISTORY_SOURCE")
    def validate_history_source(cls, v: str) -> str:
        v = (v or "ib").lower()
        if v not in ("ib", "polygon"):
            raise ValueError(f"HISTORY_SOURCE must be 'ib' or 'polygon', got {v!r}")
        return v


    class Config:
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "22_WatchlistStreamer.env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
