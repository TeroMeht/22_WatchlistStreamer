import asyncpg
import logging
from typing import Optional

from src.core.config import settings


_db_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:
    """
    Initialize the global DB pool (idempotent).
    """
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL)
        logging.info("Database pool initialized")
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
