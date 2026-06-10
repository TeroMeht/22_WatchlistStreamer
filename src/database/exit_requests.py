"""
Read-only access to the shared `exit_requests` table.

The table is owned and recreated on every startup by the 26_ReactFastApp
backend (DROP + CREATE in db/exits.py). The streamer must NOT create or
mutate it — we just read which (symbol, strategy) rows are currently
armed so run_strategies() can fire only the exits the user demanded
from the UI.

Schema mirrored from 26_ReactFastApp/backend/db/exits.py:

    CREATE TABLE exit_requests (
        symbol           TEXT NOT NULL,
        strategy         TEXT NOT NULL,
        trim_percentage  NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
        updated          TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
        PRIMARY KEY (symbol, strategy)
    );
"""
from __future__ import annotations

import logging
from typing import Dict, Set

from src.dependencies import get_db_pool

logger = logging.getLogger(__name__)


async def load_armed_exit_strategies() -> Dict[str, Set[str]]:
    """
    Return ``{SYMBOL_UPPER: {strategy_name, ...}}`` for every armed exit
    request. A symbol with no armed exits simply isn't a key in the
    result (the caller should treat a missing key as the empty set).

    The table may not exist yet if the backend hasn't booted on a fresh
    DB; in that case we log once and return an empty mapping rather than
    propagating — the streamer should keep running and pick up exits on
    the next refresh.
    """
    pool = get_db_pool()
    result: Dict[str, Set[str]] = {}

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, strategy FROM exit_requests"
            )
    except Exception as e:
        # UndefinedTableError surfaces when the backend hasn't created
        # the table yet. Don't crash the streamer over that — return
        # empty and the next refresh will succeed.
        logger.warning(
            "load_armed_exit_strategies: query failed (%s) — treating as empty",
            e,
        )
        return result

    for row in rows:
        symbol = (row["symbol"] or "").upper()
        strategy = row["strategy"]
        if not symbol or not strategy:
            continue
        result.setdefault(symbol, set()).add(strategy)

    return result
