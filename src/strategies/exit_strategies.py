from src.alarms.alarm_logics import *
from src.database.db_functions import get_last_rows
import logging
from src.helpers.utils import *
from src.alarms.send_postrequest import send_exit_request_to_fastapi
from src.core.config import settings
# ``detect_capitulation`` / ``detect_euforia`` moved out of alarm_logics
# and now live next to the reversal strategies -- import them explicitly
# so the star import above stays clean.
from src.strategies.reversal_shared.detection import (
    detect_capitulation,
    detect_euforia,
)

logger = logging.getLogger(__name__)


# NOTE: every registered exit strategy's function NAME must match its
# exit-request key exactly -- the dispatcher matches by ``fn.__name__``.


# Exit/ Trim into Relatr strength. Frontside exit point.
async def trim_into_strength(candle: CandleRow):
    logger.info("Running trim into strength exit for symbol: %s", candle.symbol)

    # Check for euforia using the DataFrame
    if candle.relatr < settings.EUFORIC_THRESHOLD:
        logger.debug("Euforia detected for symbol: %s. Checking Relatr strength...", candle.symbol)

        await send_exit_request_to_fastapi(
            candle=candle,
            alarm_name="trim_into_strength",
            fastapi_url=settings.EXIT_REQUEST_ENDPOINT
        )


# Exit/ Trim into Relatr weakness. Frontside exit point.
async def trim_into_weakness(candle: CandleRow):
    logger.info("Running trim into weakness exit for symbol: %s", candle.symbol)

    # Check for capitulation using the DataFrame
    if candle.relatr > settings.CAPITULATION_THRESHOLD:
        logger.debug("Capitulation detected for symbol: %s. Checking Relatr weakness...", candle.symbol)

        await send_exit_request_to_fastapi(
            candle=candle,
            alarm_name="trim_into_weakness",
            fastapi_url=settings.EXIT_REQUEST_ENDPOINT
        )



# Exit ema9 close crossovers
async def momentum_long_exit(candle: CandleRow):
    logger.info("Running momentum long exit for symbol: %s", candle.symbol)

    df_all = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)

    if is_crossover_down(df_all.tail(2)):
        last_row = df_all.iloc[-1]
        logging.info(f"{last_row['symbol']}: EMA9 crossover down detected")
        # Check for euforia using the DataFrame

        if detect_euforia(df_all, threshold=settings.EUFORIC_THRESHOLD):
            logger.debug("Euforia detected for symbol: %s. Generating exit signal", candle.symbol)

            await send_exit_request_to_fastapi(
                candle=candle,
                alarm_name="momentum_long_exit",
                fastapi_url=settings.EXIT_REQUEST_ENDPOINT
            )
        else:
            logger.debug("No euforia detected for symbol: %s. No exit signal generated.", candle.symbol)


async def momentum_short_exit(candle: CandleRow):
    logger.info("Running momentum short exit for symbol: %s", candle.symbol)

    df_all = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)

    if is_crossover_up(df_all.tail(2)):
        last_row = df_all.iloc[-1]
        logging.info(f"{last_row['symbol']}: EMA9 crossover up detected")

        # Check for capitulation using the DataFrame
        if detect_capitulation(df_all, threshold=settings.CAPITULATION_THRESHOLD):
            logger.debug("Capitulation detected for symbol: %s. Generating exit signal", candle.symbol)

            await send_exit_request_to_fastapi(
                candle=candle,
                alarm_name="momentum_short_exit",
                fastapi_url=settings.EXIT_REQUEST_ENDPOINT
            )
        else:
            logger.debug("No capitulation detected for symbol: %s. No exit signal generated.", candle.symbol)

async def endofday_exit(candle: CandleRow):
    logger.info("Running EoD exit for symbol: %s", candle.symbol)

    if candle.time >= settings.EOD_EXIT_TIME:
        logger.debug("EoD exit time reached for symbol: %s. Sending exit request...", candle.symbol)
        await send_exit_request_to_fastapi(
            candle=candle,
            alarm_name="endofday_exit",
            fastapi_url=settings.EXIT_REQUEST_ENDPOINT
        )


async def vwap_exit(candle: CandleRow):
    logger.info("Running VWAP exit for symbol: %s", candle.symbol)

    if is_vwap_close(candle, settings.VWAP_DISTANCE):
        logger.debug("VWAP close detected for symbol: %s. Sending exit request...", candle.symbol)
        await send_exit_request_to_fastapi(
            candle=candle,
            alarm_name="vwap_exit",
            fastapi_url=settings.EXIT_REQUEST_ENDPOINT
        )
