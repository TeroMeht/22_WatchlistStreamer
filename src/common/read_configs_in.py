"""DEPRECATED: kept for backward compatibility only.

Database configuration is now loaded from environment variables and
validated by Pydantic. Use ``src.core.config.settings.database_config``
instead.

The old ``read_database_config(filename, section)`` signature is preserved
here as a thin shim so that any straggling imports keep working during
migration. The ``filename`` and ``section`` arguments are ignored.
"""
from __future__ import annotations

import warnings

from src.core.config import settings


def read_database_config(filename: str | None = None, section: str | None = None) -> dict:
    """Deprecated. Returns ``settings.database_config``.

    The ``filename`` and ``section`` arguments are accepted for API
    compatibility with the old ``database.ini`` parser but are ignored.
    """
    warnings.warn(
        "read_database_config() is deprecated. "
        "Use `from src.core.config import settings; settings.database_config` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return settings.database_config
