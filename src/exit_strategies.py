from src.alarms.alarm_logics import *
import logging
from src.helpers.utils import *
from src.alarms.send_postrequest import send_exit_request_to_fastapi
from src.core.config import settings

logger = logging.getLogger(__name__)



async def relatr_up_exit_strategy(candle: CandleRow):

    logger.info("Running Relatr up exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="relatr_up_exit",
        fastapi_url=settings.EXIT_REQUEST_ENDPOINT
    )


async def relatr_down_exit_strategy(candle: CandleRow):

    logger.info("Running Relatr down exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="relatr_down_exit",
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