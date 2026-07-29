import asyncio

from src.common.logging_config import setup_logging
from src.dependencies import close_db_pool
from src.streamer.datastreamer import (
    data_pipe,
    initialize_app,
    prepare_database,
    prepare_watchlist,
)


# 1. Setup logging first
setup_logging()


async def main() -> None:
    ib = None
    try:
        # Process-level init: DB pool + IB connection + PID ping + dashboard.
        # Returns the connected IB client we hand to data_pipe.
        ib = await initialize_app()

        # All DB table setup in one place: alarms/orders + archive+wipe
        # of the per-symbol livestream tables.
        await prepare_database()

        # Assemble the monitor set from watchlist + armed-exit requests
        # and push it to the strategy dispatcher. None means nothing to
        # monitor -- bail before the pipeline starts.
        monitor_set = await prepare_watchlist()
        if monitor_set is None:
            return

        # Data pipeline (fetch/validate/calc/warmup) + live streamer tail.
        await data_pipe(ib, monitor_set)

    finally:
        # Always close pool + disconnect on shutdown, whether the app
        # initialized fully or bailed early.
        await close_db_pool()
        if ib is not None:
            ib.disconnect()


# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
