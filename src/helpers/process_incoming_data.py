import asyncio
import logging
import logging as _logging
from datetime import datetime
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.common.calculate import (
    calculate_next_ema9,
    calculate_next_relatr,
    calculate_next_rvol,
    calculate_next_vwap,
)
from src.core.config import settings
from src.database.db_functions import (
    fetch_avg_volume_for_candle,
    get_last_rows,
    insert_candlestick_row,
)
from src.helpers.handle_candles import (
    CandleRow,
    CandleStore,
    enforce_candle_row_types,
)
from src.helpers.utils import get_2min_interval
from src.strategies import candle_timeline
from src.strategies.orb_long.visualization import state as viz
from src.strategies.reversal_long.visualization import state as reversal_viz
from src.strategies.strategies import run_realtime_strategies, run_strategies

logger = logging.getLogger(__name__)


# --- 5-sec bar log setup -----------------------------------------------------
# Every incoming 5-sec bar is written as one CSV line to bars_5s.log.
# Isolated from the main streamer log: separate handler, propagation off,
# so nothing else pollutes the file and nothing here bleeds into the
# console. Column order:
#     timestamp_local, symbol, open, high, low, close, volume, wap, count
BARS_5S_LOG_PATH: str = "bars_5s.log"
_bars_logger = _logging.getLogger("bars_5s")
if not _bars_logger.handlers:
    _bars_logger.propagate = False
    _bars_logger.setLevel(_logging.INFO)
    _bh = _logging.FileHandler(BARS_5S_LOG_PATH, encoding="utf-8")
    _bh.setFormatter(_logging.Formatter("%(message)s"))
    _bars_logger.addHandler(_bh)







async def handle_incoming_candle(candle: CandleRow, atr_value:float) -> CandleRow:
    try:
        df_from_db, avg_volume = await asyncio.gather(
            get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None),
                          
            fetch_avg_volume_for_candle(candle)
        )
        candle.avg_volume = avg_volume
        candle = calculate_next_vwap(candle, df_from_db)
        candle = calculate_next_ema9(candle, df_from_db)
        candle = calculate_next_relatr(candle, atr_value)
        candle = calculate_next_rvol(candle,df_from_db, candle.avg_volume)
        return candle
    except Exception as e:
        logging.exception("Error in handle_next_vwap_and_ema9_values for %s: %s", candle.symbol, e)
        return candle




async def finalize_candle(last_candle,
                          atr_value: float,
                          symbol: str,
                          price):
    """Finalize previous candle, enqueue DB write, and run strategy checks."""

    # 1️ Update close price
    last_candle['close'] = price

    # 2️ Build CandleRow with known values
    last_candle = CandleRow(symbol=symbol,
                            date=last_candle["minute_dt"].date(),
                            time=last_candle["minute_dt"].time(),
                            open=last_candle["open"],
                            high=last_candle["high"],
                            low=last_candle["low"],
                            close=last_candle["close"],
                            volume=last_candle["volume"],
                            vwap=None,  # to be calculated
                            ema9=None,
                            relatR=None,
                            avg_volume=None,
                            rvol=None
                        )
    # calculations
    last_candle = await handle_incoming_candle(last_candle, atr_value)
    
    db_ready_candle = enforce_candle_row_types(last_candle)  # Ensure all floats
    logger.info(f"Finalized candle for {symbol} at {last_candle.time}: O={last_candle.open}, H={last_candle.high}, L={last_candle.low}, C={last_candle.close}, V={last_candle.volume}, VWAP={last_candle.vwap}, EMA9={last_candle.ema9}, RelatR={last_candle.relatR}, AvgVol={last_candle.avg_volume}, RVol={last_candle.rvol}")
    await insert_candlestick_row(db_ready_candle)
 
    # Push the finalized 2-min candle to the SHARED candle timeline once;
    # both per-strategy viz modules delegate to it internally.
    candle_dt = datetime.combine(db_ready_candle.date, db_ready_candle.time)
    candle_timeline.record_finalized_2min_candle(
        symbol=symbol,
        candle_dt=candle_dt,
        open_=db_ready_candle.open,
        high=db_ready_candle.high,
        low=db_ready_candle.low,
        close=db_ready_candle.close,
    )
    # Per-strategy overlays: metric each dashboard needs for its own checks.
    viz.record_rvol(symbol, db_ready_candle.rvol)            # ORB: Rvol > 3 check
    viz.record_premarket_high(                                # ORB: price >= premarket high check
        symbol, db_ready_candle.high, db_ready_candle.time,
    )  # candle_time gates the update; post-open highs are ignored
    reversal_viz.record_relatr(symbol, db_ready_candle.relatR)  # reversal: recent-capitulation check
    # NOTE: the frontend live table now polls /api/livestream/latest on a
    # 10s interval, so we no longer push each finalized candle to FastAPI.
    await run_strategies(last_candle)



async def process_bar(store: CandleStore,
                      atr_value: float,
                      symbol: str,
                      bar: RealTimeBar):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.
    """
    # --- Convert time to configured timezone here ---

    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))
    interval_time = get_2min_interval(bar_time_local)
    last_candle = store.get_last(symbol)

    logging.debug(f"New 5-sec bar for {symbol} at {bar_time_local.strftime('%H:%M:%S %Z')}: "
    f"Last= {bar.close}, Volume= {bar.volume}")

    # Persist every raw 5-sec bar to bars_5s.log (CSV).
    _bars_logger.info(
        "%s,%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%d",
        bar_time_local.strftime("%Y-%m-%d %H:%M:%S"),
        symbol,
        float(bar.open_), float(bar.high), float(bar.low), float(bar.close),
        float(bar.volume), float(bar.wap), int(bar.count),
    )

    # --- Realtime (5-sec) entry strategies ---
    # Fire per-bar strategies (e.g. ORB breakout) on every incoming 5-sec bar,
    # independent of the 2-min aggregation below. Kept before aggregation so
    # a breakout can trigger the instant price crosses the level.
    await run_realtime_strategies(bar, symbol)

    if not store.seen_minute(symbol, interval_time):
        store.add_minute(symbol, interval_time)

        if last_candle:
            # First, apply the last bar to the previous candle
            store.update_candle(symbol, bar.close, bar.volume)
            # Then finalize
            await finalize_candle(last_candle, atr_value, symbol, bar.close)

        # Start a new candle for the current interval
        store.append_candle(symbol, {
            "minute_dt": interval_time,
            "open": bar.close,
            "high": bar.close,
            "low": bar.close,
            "close": bar.close,
            "volume": 0.0
        })
    else:
        # Accumulate volume in the current candle
        store.update_candle(symbol, bar.close, bar.volume)