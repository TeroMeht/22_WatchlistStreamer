import asyncio

from src.common.logging_config import setup_logging
from src.common.read_configs_in import (
    read_database_config,
)
from src.dependencies import *
from src.streamer.datastreamer import run_streamer




# 1️⃣ Setup logging first
setup_logging()


async def main() -> None:
    """
    Main entry: loads configuration, initializes dependencies,
    and triggers the live streamer.
    """

    # Load configuration files
    database_config = read_database_config(
        filename="database.ini",
        section="livestream",
    )

    #  Initialize global DB pool (ONCE)
    await init_db_pool(database_config)
    

    try:
        # Run main streamer logic
        await run_streamer()

    finally:
        # Always close pool on shutdown
        await close_db_pool()


# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
