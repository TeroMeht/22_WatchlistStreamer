from datetime import time
from pathlib import Path
from typing import Any
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    DATABASE_URL: str

    # --- Runtime mode ---
    # "live"   -> subscribe to IB real-time bars (production).
    # "replay" -> feed bars from CSVs on disk via src/streamer/replay.py.
    # Also gates alarm dispatch (no Telegram/POST in replay) and the DB
    # pool selection (REPLAY_DATABASE_URL when set).
    MODE: str

    # --- Replay-only settings (ignored in live mode) ---
    # Symbol + date are read from the CSV rows themselves (columns
    # ``symbol`` and ``time``), so REPLAY_DATA_DIR just points at the
    # flat folder full of ``*.csv`` files.
    REPLAY_DATABASE_URL: str
    REPLAY_DATA_DIR: Path
    REPLAY_SPEED: float          # 0 = instant, 1.0 = realtime, N = Nx

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

    @field_validator("MODE")
    def validate_mode(cls, v: str) -> str:
        v = (v or "live").lower()
        if v not in ("live", "replay"):
            raise ValueError(f"MODE must be 'live' or 'replay', got {v!r}")
        return v


    class Config:
        # Centralized env-file repository (shared across all projects).
        # Runtime mode is selected via the MODE setting inside this file
        # (or overridden per-run via the MODE environment variable, which
        # pydantic-settings prefers over the file value).
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "22_WatchlistStreamer.env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
