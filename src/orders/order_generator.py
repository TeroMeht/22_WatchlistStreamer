from src.database.db_functions import *
import logging
from src.core.config import settings
from src.alarms.send_postrequest import send_entry_request_to_fastapi

logger = logging.getLogger(__name__)  # module-specific logger


def detect_stoplevel(df: pd.DataFrame, direction: str) -> float:

    if df.empty:
        raise ValueError("DataFrame is empty, cannot detect stop level")

    required_cols = {"High", "Low"}
    if not required_cols.issubset(df.columns):
        raise KeyError(f"DataFrame must contain columns: {required_cols}")

    direction = direction.lower()

    if direction == "long":
        reference_price = df["Low"].min()
        stop_level = round(reference_price - settings.ORB_STOP_OFFSET, 2)
    elif direction == "short":
        reference_price = df["High"].max()
        stop_level = round(reference_price + settings.ORB_STOP_OFFSET, 2)
    else:
        raise ValueError(f"Unsupported direction: {direction}")

    logger.info(
        "Stop level detected: direction=%s reference=%.2f offset=%.2f stop=%.2f",
        direction, reference_price, settings.ORB_STOP_OFFSET, stop_level,
    )

    return stop_level





async def generate_entry_order(candle: CandleRow, stop_level: float) -> None:
    """
    Fire the automatic entry request to FastAPI. DB persistence of the
    order is intentionally NOT done here -- the receiving service owns
    that write; keeping it out of the streamer avoids double-writes and
    means one failure mode instead of two.
    """
    try:
        await send_entry_request_to_fastapi(
            candle=candle,
            stop_level=stop_level,
            fastapi_url=settings.ENTRY_REQUEST_ENDPOINT,
        )
    except Exception:
        logger.exception(
            "Failed to POST entry request for symbol=%s date=%s time=%s stop=%s",
            candle.symbol, candle.date, candle.time, stop_level,
        )
        raise

    logger.info(
        "Entry request POSTed: symbol=%s date=%s time=%s stop=%.2f",
        candle.symbol, candle.date, candle.time, stop_level,
    )
