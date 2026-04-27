"""Core application configuration package.

Single source of truth for runtime configuration. Values are loaded
from a centralized env-repo file (``C:/codebase/env-repo/22_WatchlistStreamer.env``)
and validated with Pydantic.
"""
from src.core.config import (
    CLIENT_CONFIG,
    DATABASE_CONFIG,
    Settings,
    settings,
)

__all__ = [
    "CLIENT_CONFIG",
    "DATABASE_CONFIG",
    "Settings",
    "settings",
]
