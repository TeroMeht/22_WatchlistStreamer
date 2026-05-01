from src.alarms.alarm_logics import *
import logging
from src.helpers.utils import *
from src.alarms.send_postrequest import send_exit_request_to_fastapi
from src.core.config import CLIENT_CONFIG

logger = logging.getLogger(__name__)



async def relatr_up_exit_strategy(candle: CandleRow):

    logger.info("Running Relatr exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="relatr_up_exit",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )


async def relatr_down_exit_strategy(candle: CandleRow):

    logger.info("Running Relatr exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="relatr_down_exit",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )


async def endofday_exit_strategy(candle: CandleRow):
    logger.info("Running EoD exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="endofday_exit",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )


async def vwap_exit_strategy(candle: CandleRow):
    logger.info("Running End of Day Exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="vwap_exit",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )