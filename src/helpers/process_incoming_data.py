"""
5-sec bar -> 2-min candle aggregation + finalization + strategy fan-out.

The per-bar indicator work now lives on
``src.streamer.session_state.SymbolSessionState.apply_bar`` (which
calls the shared ``indicators`` package). ``finalize_candle`` here
just owns the boundary steps: build the ``CandleRow``, run apply_bar,
persist, and fire strategies.

The store is threaded in from ``process_bar`` -- one ``SessionStore``
per process, owned by the live/replay source.
"""
from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from data_sources._bar import IncomingBar

from src.core.config import settings
from src.database.db_functions import insert_candlestick_row
from src.helpers.handle_candles import (
    CandleRow,
    CandleStore,
    enforce_candle_row_types,
)
from src.helpers.utils import get_2min_interval
from src.strategies import candle_timeline
from src.strategies.strategies import run_realtime_strategies, run_strategies
from indicators.session_state import SessionStore

logger = logging.getLogger(__name__)

_record_5s_tick = candle_timeline.record_5s_tick


async def finalize_candle(
    last_candle: dict,
    store: SessionStore,
    symbol: str,
    price: float,
) -> None:
    """Finalize previous candle, enqueue DB write, and run strategy checks."""
    # 1. Update close price
    last_candle['close'] = price

    # 2. Build CandleRow with known values (indicator slots start None
    #    and are filled by apply_bar).
    candle = CandleRow(
        symbol      = symbol,
        date        = last_candle['minute_dt'].date(),
        time        = last_candle['minute_dt'].time(),
        open        = last_candle['open'],
        high        = last_candle['high'],
        low         = last_candle['low'],
        close       = last_candle['close'],
        volume      = last_candle['volume'],
        vwap        = None,
        ema9        = None,
        avg_volume  = None,
        rvol        = None,
        relatr      = None,
        day_atr_ext = None,
    )

    # 3. Enrich via the O(1) session-state apply_bar. Raises KeyError
    #    if the symbol wasn't seeded at boot -- which means the
    #    universe changed under us and the fix is upstream, not here.
    store.get(symbol).apply_bar(candle)

    db_ready = enforce_candle_row_types(candle)
    logger.info(
        "Finalized candle for %s at %s: O=%s H=%s L=%s C=%s V=%s "
        "VWAP=%s EMA9=%s RelatR=%s AvgVol=%s RVol=%s ATRExt=%s",
        symbol, candle.time,
        candle.open, candle.high, candle.low, candle.close, candle.volume,
        candle.vwap, candle.ema9, candle.relatr,
        candle.avg_volume, candle.rvol, candle.day_atr_ext,
    )
    await insert_candlestick_row(db_ready)

    candle_dt = datetime.combine(db_ready.date, db_ready.time)
    candle_timeline.record_finalized_2min_candle(
        symbol   = symbol,
        candle_dt= candle_dt,
        open_    = db_ready.open,
        high     = db_ready.high,
        low      = db_ready.low,
        close    = db_ready.close,
        volume   = db_ready.volume,
        vwap     = db_ready.vwap,
        ema9     = db_ready.ema9,
    )

    await run_strategies(candle)


async def process_bar(
    candle_store: CandleStore,
    session_store: SessionStore,
    symbol: str,
    bar: IncomingBar,
) -> None:
    """Aggregate incoming 5-sec bars into 2-min candles; finalize on rollover."""
    bar_time_local = bar.date.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    interval_time = get_2min_interval(bar_time_local)
    last_candle = candle_store.get_last(symbol)

    logging.debug(
        "New 5-sec bar for %s at %s: Last= %s, Volume= %s",
        symbol, bar_time_local.strftime('%H:%M:%S %Z'), bar.close, bar.volume,
    )

    _record_5s_tick(
        symbol=symbol,
        bar_dt=bar_time_local,
        open_=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
    )
    await run_realtime_strategies(bar, symbol)

    if not candle_store.seen_minute(symbol, interval_time):
        candle_store.add_minute(symbol, interval_time)

        if last_candle:
            candle_store.update_candle(symbol, bar.close, bar.volume)
            await finalize_candle(last_candle, session_store, symbol, bar.close)

        candle_store.append_candle(symbol, {
            "minute_dt": interval_time,
            "open":      bar.close,
            "high":      bar.close,
            "low":       bar.close,
            "close":     bar.close,
            "volume":    0.0,
        })
    else:
        candle_store.update_candle(symbol, bar.close, bar.volume)
