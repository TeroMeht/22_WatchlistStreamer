"""
Watchlist persistence (livestreaming DB).

Replaces the legacy tickers/watchlist.txt + symbol_loader flow with a pair of
normalized tables:

    watchlist
      id         SERIAL PK
      symbol     TEXT UNIQUE  -- stored uppercase
      created_at TIMESTAMPTZ DEFAULT NOW()

    watchlist_strategies
      id            SERIAL PK
      watchlist_id  INT FK -> watchlist(id) ON DELETE CASCADE
      strategy_name TEXT   -- must match a name registered in strategies.py
      UNIQUE (watchlist_id, strategy_name)

The 26_ReactFastApp UI is the writer (POST /api/watchlist). This module is the
streamer's *reader*: load_watchlist() returns the {symbol: {strategies}} mapping
that run_streamer() uses to decide which symbols to monitor and what each one
is allowed to trigger.
"""
from __future__ import annotations

import logging
from typing import Dict, Set

import asyncpg

from src.dependencies import get_db_pool


logger = logging.getLogger(__name__)


CREATE_WATCHLIST_SQL = """
CREATE TABLE IF NOT EXISTS watchlist (
    id          SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

CREATE_WATCHLIST_STRATEGIES_SQL = """
CREATE TABLE IF NOT EXISTS watchlist_strategies (
    id            SERIAL PRIMARY KEY,
    watchlist_id  INTEGER NOT NULL REFERENCES watchlist(id) ON DELETE CASCADE,
    strategy_name TEXT NOT NULL,
    UNIQUE (watchlist_id, strategy_name)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_watchlist_strategies_wid
    ON watchlist_strategies(watchlist_id);
"""


async def create_watchlist_tables(conn: asyncpg.Connection | None = None) -> None:
    """
    Ensure the watchlist + watchlist_strategies tables exist.

    Pass a connection if you already hold one (e.g. inside the FastAPI lifespan
    block); otherwise this acquires one from the streamer's global pool.
    """
    if conn is not None:
        await conn.execute(CREATE_WATCHLIST_SQL)
        await conn.execute(CREATE_WATCHLIST_STRATEGIES_SQL)
        await conn.execute(CREATE_INDEX_SQL)
        return

    pool = get_db_pool()
    async with pool.acquire() as c:
        await c.execute(CREATE_WATCHLIST_SQL)
        await c.execute(CREATE_WATCHLIST_STRATEGIES_SQL)
        await c.execute(CREATE_INDEX_SQL)


async def load_watchlist() -> Dict[str, Set[str]]:
    """
    Return the current watchlist as ``{SYMBOL_UPPER: {strategy_name, ...}}``.

    A symbol with zero strategies still appears in the result (mapped to an
    empty set) — the streamer will skip it because no entry strategies can
    fire, but it's not an error.
    """
    pool = get_db_pool()
    result: Dict[str, Set[str]] = {}

    async with pool.acquire() as conn:
        # Ensure tables exist before reading. Cheap and idempotent — protects
        # first runs against an unprepared database.
        await create_watchlist_tables(conn)

        rows = await conn.fetch(
            """
            SELECT w.symbol AS symbol, ws.strategy_name AS strategy_name
            FROM watchlist w
            LEFT JOIN watchlist_strategies ws ON ws.watchlist_id = w.id
            ORDER BY w.symbol ASC;
            """
        )

    for row in rows:
        symbol = (row["symbol"] or "").upper()
        if not symbol:
            continue
        bucket = result.setdefault(symbol, set())
        strat = row["strategy_name"]
        if strat:
            bucket.add(strat)

    logger.info(
        "Loaded watchlist from DB: %d symbols, %d strategy bindings",
        len(result),
        sum(len(v) for v in result.values()),
    )
    return result
