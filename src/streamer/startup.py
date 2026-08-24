"""
Streamer startup phases.

Three orchestrator steps ``main`` calls in order before it kicks off the
data pipeline:

    1. ``initialize_app``    -- process-level bring-up (DB pool,
                                 IBSource wiring, PID ping, dashboard).
    2. ``prepare_database``  -- all DB table setup in one place
                                 (alarms/orders/watchlist plus livestream
                                 rotation).
    3. ``prepare_watchlist`` -- assemble the monitor set from watchlist +
                                 armed exits and push it to the strategy
                                 dispatcher.

The IB connection itself is now owned by the shared ``data_sources.ib``
package -- this module just builds an ``IBSource`` from settings and
lets the package handle lazy connect + reconnect. The concrete
``connectAsync`` happens on first use inside the fetchers (via
``ensure_connected``), not here.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from data_sources.ib._client import IBSource, from_config as ib_source_from_config

from src.alarms.send_postrequest import send_streamer_status
from src.core.config import settings
from src.database.db_functions import (
    archive_livestream_tables,
    create_alarms_table,
    create_orders_table,
    delete_all_tables_db_async,
    create_exit_requests_table,
)
from src.database.exit_requests import load_armed_exit_strategies
from src.database.watchlist import create_watchlist_tables, load_watchlist
from src.dependencies import init_db_pool
from src.strategies.dispatcher_state import set_watchlist_strategies
from src.strategies.visualization.dashboard import start_dashboard


# =============================================================================
# Phase 1 -- initialize app
# =============================================================================


async def initialize_app() -> Optional[IBSource]:
    """
    Process-level bring-up. Returns an ``IBSource`` ready for use, OR
    ``None`` when this run doesn't need IB at all (``MODE=replay`` and
    ``HISTORY_SOURCE=polygon``).

    Skipping IBSource construction avoids ``ib_async`` even trying to
    stand up a client in cases where the streamer never touches IB --
    the whole point of the polygon warmup source (early-morning
    replays before a gateway is up).

    ``main.py``'s shutdown ``finally`` guards on ``source is not None``
    before calling ``disconnect``.
    """
    await init_db_pool()  # Initialize the global DB pool so all downstream DB calls can use it.

    skip_ib = settings.MODE == "replay" and settings.HISTORY_SOURCE == "polygon"

    if skip_ib:
        source: Optional[IBSource] = None
        logging.info(
            "IB source SKIPPED (MODE=replay HISTORY_SOURCE=polygon) -- "
            "warmup will use Polygon, bars will come from replay CSVs."
        )
    else:
        # Build the IBSource wrapper. The underlying ``IB()`` is NOT
        # connected yet -- ``ensure_connected(source)`` opens the socket
        # on first use inside the fetchers, guarded by an internal lock.
        source = ib_source_from_config(settings)
        logging.info(
            "IBSource ready (lazy connect): %s:%d clientId=%s",
            source.host, source.port, source.client_id,
        )

    # Notify the backend we're up (PID lets it watch for hard kills).
    await send_streamer_status(
        settings.STREAMER_START_ENDPOINT,
        label="start",
        payload={"pid": os.getpid()},
    )

    # Bring up the unified strategy dashboard (localhost:8790).
    await start_dashboard()

    return source


# =============================================================================
# Phase 2 -- database preparation
# =============================================================================


async def prepare_database() -> None:

    await create_alarms_table()
    await create_orders_table()
    await create_watchlist_tables()
    await create_exit_requests_table()
    # Skip archiving in replay mode: replay bars would otherwise pollute
    # bars_2m_archive with rows that look like real trading data.
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
    another DB lookup.
    """
    set_watchlist_strategies(monitor_set)
