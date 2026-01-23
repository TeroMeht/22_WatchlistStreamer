import asyncpg
import logging
from typing import Optional


_db_pool: Optional[asyncpg.Pool] = None


async def init_db_pool(database_config: dict):
    """
    Initialize the global asyncpg connection pool.
    Call this ONCE at app startup.
    """
    global _db_pool

    if _db_pool is not None:
        return _db_pool

    try:
        _db_pool = await asyncpg.create_pool(
            user=database_config["user"],
            password=database_config["password"],
            database=database_config["database"],
            host=database_config["host"],
            port=int(database_config.get("port", 5432)),
            min_size=1,
            max_size=10,
        )
        logging.info("Database pool initialized")
        return _db_pool

    except Exception as e:
        logging.exception("Failed to initialize DB pool: %s", e)
        raise


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


