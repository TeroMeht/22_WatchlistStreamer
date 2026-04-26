import asyncio

from src.common.logging_config import setup_logging
from src.common.read_configs_in import (
    read_database_config,
)
from src.dependencies import *
from src.streamer.datastreamer import run_streamer
from src.database.db_functions import create_alarms_table, create_orders_table 
from config import CLIENT_CONFIG
from ib_async import IB


# 1️⃣ Setup logging first
setup_logging()


async def main() -> None:

    ib = IB()
    await ib.connectAsync(CLIENT_CONFIG["host"],
                          CLIENT_CONFIG["port"],
                          CLIENT_CONFIG["clientId"])
    
    # Load configuration files
    database_config = read_database_config(
        filename="database.ini",
        section="livestream",
    )

    # Initialize global DB pool (ONCE)
    await init_db_pool(database_config)

    # Ensure tables exist before any inserts/reads happen
    pool = get_db_pool()
    async with pool.acquire() as conn:
        await create_alarms_table(conn)
        await create_orders_table(conn)
        logging.info("Tables ensured: alarms, orders")


    try:
        # Run main streamer logic
        await run_streamer(ib)

    finally:
        # Always close pool on shutdown
        await close_db_pool()
        ib.disconnect()

# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
