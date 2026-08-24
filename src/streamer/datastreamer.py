from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
import logging

import aiohttp

from src.core.config import settings
from src.database.db_functions import (
    create_and_fill_avg_volume_tables_async,
    create_and_fill_table_async,
)
from src.helpers.handle_dataframes import (
    bars_to_avg_volume_frame,
    bars_to_today_frame,
    handle_Atr_intraday_dataset,
    handle_incoming_dataframe_daily,
    handle_incoming_dataframe_intradays_volume,
    handle_intraday_rvol_dataset,
)
# IB fetchers + realtime subscribe come from the shared data_sources
# package (C:/codebase/prod/data_sources). This file no longer knows
# about ``ib_async`` or the old ``src.helpers.ibclient`` module.
#
# We import from the concrete modules rather than from ``data_sources.ib``
# because the subpackage's ``__init__.py`` intentionally re-exports
# nothing -- keeps the top-level import graph light.
from data_sources.ib._client import IBSource
from data_sources.ib.historical import fetch_daily, fetch_intraday
from data_sources.ib.live import subscribe_realtime
from src.helpers.process_incoming_data import CandleStore, process_bar
from src.strategies import warmup
from src.streamer import replay
from src.streamer.datavalidation import validate_tickers
from src.streamer.replay import get_replay_start_datetime

# =============================================================================
# Phase 4 -- history data fetch (three-way split)
# =============================================================================


def _make_dict(bars_list, tickers, transform_fn) -> dict:
    """
    Zip each ticker with its fetched bars, drop None-bar fetches, apply
    the transform, drop empty DataFrames. Returns ``{symbol: DataFrame}``.

    Same shape used for daily / past / today so the three flows don't
    drift when one of them changes.
    """
    out: dict = {}
    for bars, sym in zip(bars_list, tickers):
        if not bars:
            continue
        df = transform_fn(bars, sym)
        if df is not None and not df.empty:
            out[sym] = df
    return out


def _to_ib_utc(dt: datetime) -> str:
    """
    IB unambiguous UTC endDateTime wire format: ``YYYYMMDD-HH:MM:SS``.
    Prefer this over the space-separated form (which IB treats as the
    account's local wall clock -- undefined behavior across DST).
    """
    return dt.astimezone(ZoneInfo("UTC")).strftime("%Y%m%d-%H:%M:%S")


@dataclass
class _FetchAnchors:
    """
    End-of-window cutoffs for the three warmup fetches, already
    formatted for IB's ``endDateTime`` wire spec. Kept as a tiny
    dataclass so callers pass one thing, not three positional strings.

    * ``daily_end_str``  -- 'YYYYMMDD 23:59:59', end of the daily
                            window (yesterday or replay-day-1).
    * ``past_end_str``   -- 'YYYYMMDD-HH:MM:SS' UTC, start of the
                            live/replay session -- upper bound for the
                            5-day intraday history.
    * ``live_end_str``   -- 'YYYYMMDD-HH:MM:SS' UTC OR '' (IB's
                            convention for 'now') -- upper bound for
                            today's 2-min session-so-far.
    """
    daily_end_str: str
    past_end_str:  str
    live_end_str:  str


def _data_fetch_anchors() -> _FetchAnchors:
    """
    Build the three end-of-window cutoffs from mode + replay state.

    * live mode   -> daily ends yesterday 23:59; past ends today 00:00
                     (Helsinki); live ends 'now' (IB: empty string).
    * replay mode -> anchored to ``get_replay_start_datetime()``: daily
                     ends (replay_date - 1) 23:59; past ends replay_date
                     00:00; live ends at the replay-start moment.
    """
    tz = ZoneInfo(settings.TIMEZONE)

    if settings.MODE == "replay":
        replay_start = get_replay_start_datetime()
        daily_end_str = (replay_start.date() - timedelta(days=1)).strftime('%Y%m%d 23:59:59')
        past_end_dt   = datetime.combine(replay_start.date(), time(0, 0), tzinfo=tz)
        live_end_dt   = replay_start
    else:
        now = datetime.now(tz=tz)
        daily_end_str = (now - timedelta(days=1)).strftime('%Y%m%d 23:59:59')
        past_end_dt   = datetime.combine(now.date(), time(0, 0), tzinfo=tz)
        live_end_dt   = None    # None -> "" (IB convention for "now")

    return _FetchAnchors(
        daily_end_str = daily_end_str,
        past_end_str  = _to_ib_utc(past_end_dt),
        live_end_str  = "" if live_end_dt is None else _to_ib_utc(live_end_dt),
    )


async def _fetch_from_ib(
    source: IBSource, tickers: list, anchors: _FetchAnchors,
) -> tuple[dict, dict, dict]:

    daily_bars_list, past_bars_list, today_bars_list = await asyncio.gather(
        asyncio.gather(*(fetch_daily(
            source, t,
            end_dt_str=anchors.daily_end_str,
            duration_days=14,
            bar_size="1 day",
        ) for t in tickers)),
        asyncio.gather(*(fetch_intraday(
            source, t,
            end_dt_str=anchors.past_end_str,
            duration_days=5,
            bar_size="2 mins",
        ) for t in tickers)),
        asyncio.gather(*(fetch_intraday(
            source, t,
            end_dt_str=anchors.live_end_str,
            duration_days=1,
            bar_size="2 mins",
        ) for t in tickers)),
    )

    daily = _make_dict(daily_bars_list, tickers, handle_incoming_dataframe_daily)
    past  = _make_dict(past_bars_list,  tickers, bars_to_avg_volume_frame)
    today = _make_dict(today_bars_list, tickers, bars_to_today_frame)
    return daily, today, past


async def _fetch_from_polygon(tickers: list) -> tuple[dict, dict, dict]:

    from src.helpers import polygon_history
    from src.helpers.polygon_client import PolygonClient

    timeout = aiohttp.ClientTimeout(total=30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = PolygonClient(
            session   = session,
            api_key   = settings.POLYGON_API_KEY,
            base_url  = settings.POLYGON_BASE_URL.rstrip("/"),
        )
        daily_dfs, intraday_pairs = await asyncio.gather(
            asyncio.gather(*(polygon_history.fetch_history_daily(client, t)
                             for t in tickers)),
            asyncio.gather(*(polygon_history.fetch_intraday_volume_history(client, t)
                             for t in tickers)),
        )

    def _ok(df) -> bool:
        return df is not None and not df.empty

    daily = {t: d    for t, d in zip(tickers, daily_dfs)     if _ok(d)}
    today = {t: p[0] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[0])}
    past  = {t: p[1] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[1])}
    return daily, today, past


# =============================================================================
# Phase 6 -- indicator calculations (Rvol / ATR / Relatr)
# =============================================================================


def _calculate_indicators(
    daily_data: list,
    today_intradaydata: list,
    past_intradaydata: list,
) -> tuple:

    rvol_dataset = handle_intraday_rvol_dataset(today_intradaydata, past_intradaydata)
    relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(rvol_dataset, daily_data)
    return relatr_datasets, last_atr_dict


# =============================================================================
# Phase 7 -- persist indicator datasets
# =============================================================================


async def _fill_database_tables_with_enriched_data(
    relatr_datasets: dict,
    past_intradaydata: dict,
) -> None:
    """
    Write the enriched intraday history to the DB tables the live path
    reads from:
        * average-volume tables (one per ticker) written from the 5-day
          intraday-volume frames -- read by the live path to compute Rvol
          per finalized 2-min candle.
        * per-ticker ``*_livestream`` tables seeded with the enriched
          intraday history -- the base rows every strategy reads from.

    Both sets of writes run concurrently.
    """
    await asyncio.gather(
        create_and_fill_avg_volume_tables_async(list(past_intradaydata.values())),
        *(create_and_fill_table_async(df) for df in relatr_datasets.values()),
    )


# =============================================================================
# Phase 8 -- live data streamer
# =============================================================================


async def run_streamer(source, valid_tickers: list, last_atr_dict: dict) -> None:

    candle_store = CandleStore()

    if settings.MODE == "replay":
        logging.info(
            "Starting replay mode (speed=%s, data_dir=%s)...",
            settings.REPLAY_SPEED, settings.REPLAY_DATA_DIR,
        )
        await replay.run_replay(candle_store, last_atr_dict, valid_tickers)
        return

    logging.info("Starting live monitoring...")


    def _make_handler(sym: str, atr):
        async def _handler(bar, symbol: str) -> None:
            await process_bar(candle_store, atr, symbol, bar)
        return _handler

    await asyncio.gather(*[
        subscribe_realtime(source, t, _make_handler(t, last_atr_dict.get(t)))
        for t in valid_tickers
    ])


# =============================================================================
# Orchestrator
# =============================================================================


async def data_pipe(source, monitor_set: dict) -> None:

    tickers = sorted(monitor_set)

    # Pick warmup source. Anchors are computed once and passed to whichever
    # per-source fetcher runs -- polygon ignores them today; IB uses all three.
    anchors = _data_fetch_anchors()
    if settings.HISTORY_SOURCE == "ib":
        daily_data, today_intradaydata, past_intradaydata = await _fetch_from_ib(
            source, tickers, anchors,
        )
    elif settings.HISTORY_SOURCE == "polygon":
        daily_data, today_intradaydata, past_intradaydata = await _fetch_from_polygon(
            tickers,
        )
    else:
        raise ValueError(f"unknown HISTORY_SOURCE: {settings.HISTORY_SOURCE!r}")

    valid_tickers, daily_data, today_intradaydata, past_intradaydata = validate_tickers(
        daily_data, today_intradaydata, past_intradaydata, tickers,
    )
    if not valid_tickers:
        logging.error("No valid tickers found in all datasets. Aborting.")
        return

    relatr_datasets, last_atr_dict = _calculate_indicators(
        daily_data, today_intradaydata, past_intradaydata,
    )

    await _fill_database_tables_with_enriched_data(relatr_datasets, past_intradaydata)

    # Seed strategies' in-memory state from the enriched history. One-time

    warmup.warmup_from_intraday(relatr_datasets)
    warmup.warmup_from_daily(daily_data)

    await run_streamer(source, valid_tickers, last_atr_dict)
