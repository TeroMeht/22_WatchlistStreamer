import asyncio

from data_sources.ib._client import disconnect as ib_disconnect

from src.common.logging_config import setup_logging
from src.dependencies import close_db_pool
from src.streamer.datastreamer import data_pipe
from src.streamer.startup import (
    initialize_app,
    prepare_database,
    prepare_watchlist,
    register_monitor_set,
)


# 1. Setup logging first
setup_logging()


async def main() -> None:
    source = None
    try:
        # Process-level init: DB pool + IBSource (lazy-connect) + PID
        # ping + dashboard. ``source`` is None when this run doesn't
        # need IB at all (replay + polygon combo).
        source = await initialize_app()

        # All DB table setup in one place: alarms/orders + archive+wipe
        # of the per-symbol livestream tables.
        await prepare_database()

        # Assemble the monitor set from watchlist + armed-exit requests.
        # None means nothing to monitor -- bail before the pipeline starts.
        monitor_set = await prepare_watchlist()
        if monitor_set is None:
            return

        # Publish the assembled mapping to the strategy dispatcher so
        # run_strategies() can filter per ticker.
        register_monitor_set(monitor_set)

        # Data pipeline (fetch/validate/calc/warmup) + live streamer tail.
        await data_pipe(source, monitor_set)

    finally:
        # Always close pool + disconnect on shutdown, whether the app
        # initialized fully or bailed early.
        await close_db_pool()
        if source is not None:
            ib_disconnect(source)


# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
