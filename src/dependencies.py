import asyncpg
import logging
from typing import Optional

from src.core.config import settings


_db_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:

    global _db_pool
    if _db_pool is None:
        if settings.MODE == "replay":
            dsn = settings.REPLAY_DATABASE_URL
            if not dsn:
                raise RuntimeError(
                    "MODE=replay but REPLAY_DATABASE_URL is empty. "
                    "Set it in your .test.env before starting."
                )
        else:
            dsn = settings.DATABASE_URL
        _db_pool = await asyncpg.create_pool(dsn=dsn)
        logging.info("Database pool initialized (mode=%s)", settings.MODE)
    return _db_pool


def get_db_pool() -> asyncpg.Pool:
    """
    Get the initialized DB pool.
    """
    if _db_pool is None:
        raise RuntimeError("DB pool not initialized. Call init_db_pool() first.")
    return _db_pool


async def close_db_pool():
    """
    Gracefully close the pool on shutdown.
    """
    global _db_pool
    if _db_pool:
        await _db_pool.close()
        _db_pool = None
        logging.info("Database pool closed")
