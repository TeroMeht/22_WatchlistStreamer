from src.alarms.alarm_logics import *
import logging
from src.helpers.utils import *
from src.alarms.send_postrequest import send_exit_request_to_fastapi
from src.core.config import settings

logger = logging.getLogger(__name__)



async def momentum_long_exit(last_8_rows: pd.DataFrame,candle: CandleRow):

    logger.info("Running momentum long exit for symbol: %s", candle.symbol)

    # Check for euforia using the DataFrame
    if detect_euforia(last_8_rows, threshold=settings.CAPITULATION_THRESHOLD):
        logger.debug("Euforia detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_down(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover down detected, generating alarm...")

            await send_exit_request_to_fastapi(
                candle=candle,
                alarm_name="momentum_long_exit",
                fastapi_url=settings.EXIT_REQUEST_ENDPOINT
            )


async def momentum_short_exit(last_8_rows: pd.DataFrame, candle: CandleRow):

    logger.info("Running momentum short exit for symbol: %s", candle.symbol)

    # Check for capitulation using the DataFrame
    if detect_capitulation(last_8_rows, threshold=settings.CAPITULATION_THRESHOLD):
        logger.debug("Capitulation detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_up(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            await send_exit_request_to_fastapi(
                candle=candle,
                alarm_name="momentum_short_exit",
                fastapi_url=settings.EXIT_REQUEST_ENDPOINT
            )


async def endofday_exit_strategy(candle: CandleRow):
    logger.info("Running EoD exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="endofday_exit",
        fastapi_url=settings.EXIT_REQUEST_ENDPOINT
    )


async def vwap_exit_strategy(candle: CandleRow):
    logger.info("Running vwap exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="vwap_exit",
        fastapi_url=settings.EXIT_REQUEST_ENDPOINT
    )

