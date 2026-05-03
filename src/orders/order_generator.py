from src.database.db_functions import *
import logging

logger = logging.getLogger(__name__)  # module-specific logger


def detect_stoplevel(df: pd.DataFrame, direction: str, offset: float = 0.10) -> float:

    if df.empty:
        raise ValueError("DataFrame is empty, cannot detect stop level")

    required_cols = {"High", "Low"}
    if not required_cols.issubset(df.columns):
        raise KeyError(f"DataFrame must contain columns: {required_cols}")

    direction = direction.lower()

    if direction == "long":
        reference_price = df["Low"].min()
        stop_level = round(reference_price - offset, 2)
    elif direction == "short":
        reference_price = df["High"].max()
        stop_level = round(reference_price + offset, 2)
    else:
        raise ValueError(f"Unsupported direction: {direction}")

    logger.info(
        "Stop level detected: direction=%s reference=%.2f offset=%.2f stop=%.2f",
        direction, reference_price, offset, stop_level,
    )

    return stop_level





async def generate_entry_order(candle: CandleRow, stop_level: float) -> None:
    try:
        # Insert active order to DB
        await insert_order(candle=candle, stop_level=stop_level)
        logger.info(
            "Entry order inserted: symbol=%s date=%s time=%s stop=%.2f",
            candle.symbol, candle.date, candle.time, stop_level,
        )
    except Exception:
        logger.exception(
            "Failed to insert entry order for symbol=%s date=%s time=%s stop=%s",
            candle.symbol, candle.date, candle.time, stop_level,
        )
        raise
