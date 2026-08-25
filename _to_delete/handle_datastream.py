import asyncio
import logging
from src.common.calculate import (
    calculate_next_day_atr_ext,
    calculate_next_ema9,
    calculate_next_relatr,
    calculate_next_rvol,
    calculate_next_vwap,
)
from src.helpers.handle_candles import CandleRow

from src.database.db_functions import (
    fetch_avg_volume_for_candle,
    get_last_rows,
)
logger = logging.getLogger(__name__)

async def handle_incoming_candle(
    candle: CandleRow, atr_value: float, prev_close: float,
) -> CandleRow:
    try:
        df_from_db, avg_volume = await asyncio.gather(
            get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None),

            fetch_avg_volume_for_candle(candle)
        )
        candle.avg_volume = avg_volume
        candle = calculate_next_vwap(candle, df_from_db)
        candle = calculate_next_ema9(candle, df_from_db)
        candle = calculate_next_relatr(candle, atr_value)
        candle = calculate_next_day_atr_ext(candle, atr_value, prev_close)
        candle = calculate_next_rvol(candle,df_from_db, candle.avg_volume)
        return candle
    except Exception as e:
        logging.exception("Error in handle_next_vwap_and_ema9_values for %s: %s", candle.symbol, e)
        return candle