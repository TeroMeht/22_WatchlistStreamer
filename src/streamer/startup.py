"""
Streamer startup phases.

Three orchestrator steps ``main`` calls in order before it kicks off the
data pipeline:

    1. ``initialize_app``    -- process-level bring-up (DB pool, IB
                                 connection, PID ping, dashboard).
    2. ``prepare_database``  -- all DB table setup in one place
                                 (alarms/orders/watchlist plus livestream
                                 rotation).
    3. ``prepare_watchlist`` -- assemble the monitor set from watchlist +
                                 armed exits and push it to the strategy
                                 dispatcher.

Kept in their own module so ``datastreamer.py`` can stay focused on the
data pipeline + live loop; each phase has a single well-defined
responsibility that's easy to read from ``main.py``.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from ib_async import IB

from src.alarms.send_postrequest import send_streamer_status
from src.core.config import settings
from src.database.db_functions import (
    archive_livestream_tables,
    create_alarms_table,
    create_orders_table,
    delete_all_tables_db_async,
    create_exit_requests_table
)
from src.database.exit_requests import load_armed_exit_strategies
from src.database.watchlist import create_watchlist_tables, load_watchlist
from src.dependencies import init_db_pool
from src.strategies.dispatcher_state import set_watchlist_strategies
from src.strategies.visualization.dashboard import start_dashboard


# =============================================================================
# Phase 1 -- initialize app
# =============================================================================


async def initialize_app() -> IB:

    await init_db_pool() # Initialize the global DB pool so all downstream DB calls can use it.

    # Open the IB gateway connection. Fail fast here -- nothing downstream
    # works without a live IB session.
    ib = IB()
    await ib.connectAsync(
        settings.IB_HOST,
        settings.IB_PORT,
        clientId=settings.IB_CLIENT_ID,
    )
    logging.info(
        "IB connection established: %s:%d (clientId=%s)",
        settings.IB_HOST, settings.IB_PORT, settings.IB_CLIENT_ID,
    )

    # Notify the backend we're up (PID lets it watch for hard kills).
    await send_streamer_status(
        settings.STREAMER_START_ENDPOINT,
        label="start",
        payload={"pid": os.getpid()},
    )

    # Bring up the unified strategy dashboard (localhost:8790). One page
    # renders overlays from every strategy's viz state module. Non-fatal
    # if aiohttp isn't installed -- start_dashboard logs and returns None.
    await start_dashboard()

    return ib


# =============================================================================
# Phase 2 -- database preparation
# =============================================================================


async def prepare_database() -> None:

    await create_alarms_table()
    await create_orders_table()
    await create_watchlist_tables()
    await create_exit_requests_table()
    # Skip archiving in replay mode: replay bars would otherwise pollute
    # bars_2m_archive with rows that look like real trading data. In
    # replay we're pointing at a separate DB anyway, so there's nothing
    # to preserve across the wipe.
    if settings.MODE != "replay":
        await archive_livestream_tables()
    await delete_all_tables_db_async()


# =============================================================================
# Phase 3 -- prepare watchlist (monitor set assembly)
# =============================================================================


async def prepare_watchlist() -> Optional[dict]:
    watchlist   = await load_watchlist()
    armed_exits = await load_armed_exit_strategies()

    monitor_set = dict(watchlist)                     # start from watchlist

    for symbol in armed_exits:
        monitor_set.setdefault(symbol, set())         # pull in exit-only symbols

    if not monitor_set:
        logging.warning("Nothing to monitor: watchlist is empty AND no armed exit requests exist.")
        return None

    return monitor_set


def register_monitor_set(monitor_set: dict) -> None:
    """
    Push the assembled monitor set to the strategy dispatcher so
    ``run_strategies()`` can filter entry strategies per ticker without
    another DB lookup. Separated from ``prepare_watchlist`` to keep the
    "read + merge" step free of side effects; the caller runs this after
    checking that ``prepare_watchlist`` returned a non-empty mapping.
    """
    set_watchlist_strategies(monitor_set)
