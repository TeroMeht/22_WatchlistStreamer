from src.common.logging_config import setup_logging
import logging
import asyncio

from src.common.read_configs_in import read_database_config, read_project_config
from src.streamer.datastreamer import run_streamer
from src.symbol_loader.loader import load_symbols_from_folder

from dash import Dash
from dash_bootstrap_components.themes import BOOTSTRAP

# 1️⃣ Setup logging first
setup_logging()
logger = logging.getLogger("main")


# --- Main entry point ---
async def main()-> None:
    """
    Main entry: loads configuration and triggers the live streamer.
    """
    # Load configuration files
    database_config = read_database_config(filename="database.ini", section="livestream")
    project_config = read_project_config(config_file="config.json")


    # Folder containing tickers and ib_connection.json
    tickers_folder = "./tickers"

    # Load symbols
    symbols = load_symbols_from_folder(tickers_folder)
    logger.info(f"Loaded symbols: {symbols}")
    if not symbols:
        logger.warning("No symbols found. Exiting.")
        return

    # Call the streamer function
    await run_streamer(symbols, project_config, database_config)


# --- Script execution ---
if __name__ == "__main__":
    asyncio.run(main())
