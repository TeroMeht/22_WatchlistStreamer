import asyncio
import logging
import logging as _logging
from datetime import datetime
from zoneinfo import ZoneInfo

from data_sources._bar import IncomingBar

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
from src.strategies.strategies import run_realtime_strategies, run_strategies

logger = logging.getLogger(__name__)

_record_5s_tick = candle_timeline.record_5s_tick





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
    logger.debug(f"Finalized candle for {symbol} at {last_candle.time}: O={last_candle.open}, H={last_candle.high}, L={last_candle.low}, C={last_candle.close}, V={last_candle.volume}, VWAP={last_candle.vwap}, EMA9={last_candle.ema9}, RelatR={last_candle.relatR}, AvgVol={last_candle.avg_volume}, RVol={last_candle.rvol}")
    await insert_candlestick_row(db_ready_candle)

    # Push the finalized 2-min candle to the SHARED candle timeline once;
    # both per-strategy viz modules delegate to it internally. VWAP and
    # EMA9 ride along on the record so the dashboard can plot them as
    # overlay line series without needing a second store.
    candle_dt = datetime.combine(db_ready_candle.date, db_ready_candle.time)
    candle_timeline.record_finalized_2min_candle(
        symbol=symbol,
        candle_dt=candle_dt,
        open_=db_ready_candle.open,
        high=db_ready_candle.high,
        low=db_ready_candle.low,
        close=db_ready_candle.close,
        vwap=db_ready_candle.vwap,
        ema9=db_ready_candle.ema9,
    )
    # Per-strategy overlays: neither strategy needs per-candle overlay
    # writes anymore. All filter values on both cards come from
    # ``filters.evaluate_filters`` on every 5-sec tick and are pushed to
    # the respective card via ``viz.record_filter_results``.
    # NOTE: the frontend live table now polls /api/livestream/latest on a
    # 10s interval, so we no longer push each finalized candle to FastAPI.
    await run_strategies(last_candle)



async def process_bar(store: CandleStore,
                      atr_value: float,
                      symbol: str,
                      bar: IncomingBar):
    """
    Process incoming 5-sec bar into aggregated 2-min candlesticks.

    ``bar`` is a canonical ``IncomingBar``. Both the IB live path
    (converted from ``ib_async.RealTimeBar`` inside
    ``IBRealtimeSource``) and the CSV replay path produce it. For
    realtime bars ``bar.date`` holds a naive-UTC datetime (matches
    the ``RealTimeBar.time`` shape the adapter reads from).
    """
    # --- Convert time to configured timezone here ---

    bar_time_local = bar.date.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(settings.TIMEZONE))
    interval_time = get_2min_interval(bar_time_local)
    last_candle = store.get_last(symbol)

    logging.debug(f"New 5-sec bar for {symbol} at {bar_time_local.strftime('%H:%M:%S %Z')}: "
    f"Last= {bar.close}, Volume= {bar.volume}")


    _record_5s_tick(
        symbol=symbol,
        bar_dt=bar_time_local,
        open_=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
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
