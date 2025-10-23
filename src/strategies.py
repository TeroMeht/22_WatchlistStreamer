from src.alarms.alarm_logics import *
from src.database.db_functions import get_last_rows
import asyncio
import logging

logger = logging.getLogger(__name__)





async def reversal_strategy(candle, database_config, project_config):
    """
    Reversal long strategy:
    - detect capitulation
    - trigger EMA9 crossover up
    """
    symbol = candle[0].lower()
    logger.info("Running Reversal Long strategy for symbol: %s", candle[0])

    # Fetch last 5 rows once
    df = await get_last_rows(symbol, num_rows=5, database_config=database_config)

    # Check for capitulation using the DataFrame
    if detect_capitulation(df, threshold=project_config["capitulation_threshold"]):
        logger.info("Capitulation detected for symbol: %s. Checking EMA9 crossover...", symbol)

        # Check EMA9 crossover using last 2 rows from the same DataFrame
        await detect_ema_crossover_up(
            df.tail(2),
            table_name=symbol,
            database_config=database_config,
            project_config=project_config
        )
    else:
        logger.info("No capitulation detected for symbol: %s", symbol)


async def reversal_short_strategy(candle, database_config, project_config):
    """
    Reversal short strategy:
    - detect euphoria
    - trigger EMA9 crossover down
    """
    symbol = candle[0].lower()
    logger.info("Running Reversal Short strategy for symbol: %s", candle[0])

    # Fetch last 5 rows once
    df = await get_last_rows(symbol, num_rows=5, database_config=database_config)

    # Check for euphoria using the DataFrame
    if detect_euforia(df, threshold=project_config["capitulation_threshold"]):
        logger.info("Euphoria detected for symbol: %s. Checking EMA9 crossover down...", symbol)

        # Check EMA9 crossover using last 2 rows from the same DataFrame
        await detect_ema_crossover_down(
            df.tail(2),
            table_name=symbol,
            database_config=database_config,
            project_config=project_config
        )
    else:
        logger.info("No euphoria detected for symbol: %s", symbol)


async def vwapcontinuation_strategy(candle, database_config, project_config):
    """
    VWAP Continuation strategy:
    - first check VWAP closeness
    - then detect past euforia
    - if euforia detected, trigger VWAP setup alarm
    """
    symbol = candle[0].lower()
    logger.info("Running VWAP Continuation strategy for symbol: %s", candle[0])

    # Check VWAP closeness first (latest row)
    df_latest = await get_last_rows(symbol, num_rows=1, database_config=database_config)
    if not is_vwap_close(df_latest, project_config["vwap_distance"]):
        logger.info("Price not close to VWAP for symbol: %s, skipping euforia check.", symbol)
        return

    logger.info(f"{symbol}: Price is close to VWAP, checking for past euforia...")

    # Fetch all historical rows
    df_all = await get_last_rows(symbol, num_rows=None, database_config=database_config)


    # Detect euforia
    if detect_euforia(df_all, threshold=project_config["capitulation_threshold"]):
        logger.info(f"Euforia detected for symbol: {symbol} near VWAP, triggering VWAP setup alarm...")

        # Trigger VWAP setup alarm
        await detect_vwap_setup(
            df=df_all,
            table_name = symbol,
            database_config=database_config,
            project_config=project_config,

        )
    else:
        logger.info(f"No euforia detected for symbol: {symbol} near VWAP.")






# def exit_strategy(candle, database_config, project_config):
#     """
#     Exit strategy:
#     - Trigger EMA9 crossover down for exiting positions
#     """
#     symbol = candle[0].lower()
#     logger.info("Running Exit strategy for symbol: %s", candle[0])

    
#     detect_ema9crossover_down(
#         symbol,
#         num_rows=2,
#         database_config=database_config,
#         project_config=project_config
#     )


async def run_strategies(project_config,
                   database_config, 
                   last_candle
                   ):
    

    """Run all trading strategies on the finalized candle."""


    await asyncio.gather(
        reversal_strategy(last_candle, database_config, project_config),
        vwapcontinuation_strategy(last_candle, database_config, project_config),
        # reversal_short_strategy(last_candle, database_config, project_config),
        # exit_strategy(last_candle, database_config, project_config),
    )
   # await reversal_short_strategy(last_candle, database_config, project_config)
    # exit_strategy(last_candle, database_config, project_config)
