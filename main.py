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

        source, warmup, live = await initialize_app() # Täällä päätetään data source configin mukaan

        await prepare_database()

        monitor_set = await prepare_watchlist()
        if monitor_set is None:
            return

        register_monitor_set(monitor_set)

        await data_pipe(warmup, live, monitor_set)

    finally:

        await close_db_pool()
        if source is not None:
            ib_disconnect(source)


# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
