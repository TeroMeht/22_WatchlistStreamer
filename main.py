import asyncio

from src.common.logging_config import setup_logging
from src.dependencies import *
from src.streamer.datastreamer import run_streamer
from src.database.db_functions import create_alarms_table, create_orders_table
from src.core.config import settings
from ib_async import IB


# 1️⃣ Setup logging first
setup_logging()


async def main() -> None:

    ib = IB()
    await ib.connectAsync(
        settings.IB_HOST,
        settings.IB_PORT,
        settings.IB_CLIENT_ID,
    )

    # Initialize global DB pool (ONCE) using validated DB config from .env
    await init_db_pool(settings.database_config)

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
