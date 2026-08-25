from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp

from data_sources._base      import BarSize, HistoryWindow
from data_sources.ib._client import IBSource
from data_sources.ib._source import IBHistoricalSource

from src.core.config import settings
from src.helpers.handle_dataframes import (
    bars_to_avg_volume_frame,
    bars_to_today_frame,
    handle_incoming_dataframe_daily,
)
from src.streamer.replay import get_replay_start_datetime


logger = logging.getLogger(__name__)


# =============================================================================
# Contract
# =============================================================================


class WarmupSource(ABC):
    """
    Project-local abstraction over 'fetch the three warmup DataFrames'.
    ``data_pipe`` holds a ``WarmupSource``, not an IB or Polygon type;
    the branch on ``settings.HISTORY_SOURCE`` lives in
    ``make_warmup_source``.
    """

    @abstractmethod
    async def fetch(
        self, tickers: list[str],
    ) -> tuple[dict, dict, dict]:
        """
        Returns ``(daily_data, today_intradaydata, past_intradaydata)``,
        each a ``{symbol: DataFrame}`` ready for
        ``_calculate_indicators``. Empty frames are filtered out; the
        caller narrows tickers via ``validate_tickers`` after.
        """


def _transform_bars_dict(bars_map: dict, transform_fn) -> dict:
    """
    Apply ``transform_fn(bars, sym)`` to each entry in ``bars_map`` and
    drop empty DataFrames. ``bars_map`` is what
    ``HistoricalSource.fetch_many`` returns -- already excludes symbols
    with no bars, so no None-check needed here.
    """
    out: dict = {}
    for sym, bars in bars_map.items():
        df = transform_fn(bars, sym)
        if df is not None and not df.empty:
            out[sym] = df
    return out


def _window_ends() -> tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:

    tz = ZoneInfo(settings.TIMEZONE)
    if settings.MODE == "replay":
        replay_start = get_replay_start_datetime()
        replay_date = replay_start.date()
        daily_end = datetime.combine(replay_date - timedelta(days=1), time(23, 59, 59), tzinfo=tz)
        past_end  = datetime.combine(replay_date, time(0, 0), tzinfo=tz)
        today_end = replay_start
    else:
        now = datetime.now(tz=tz)
        daily_end = datetime.combine(now.date() - timedelta(days=1), time(23, 59, 59), tzinfo=tz)
        past_end  = datetime.combine(now.date(), time(0, 0), tzinfo=tz)
        today_end = None
    return daily_end, past_end, today_end


# =============================================================================
# IB implementation
# =============================================================================


class IBWarmupSource(WarmupSource):
    """
    Warmup driven by ``IBHistoricalSource``.

    Three ``fetch_many`` calls (daily 14D useRTH, past 5D 2-min,
    today-so-far 2-min) then apply the DataFrame transforms. The IB
    ``BarSize`` / ``HistoryWindow.end`` mapping happens inside the
    adapter -- this class only speaks the source-agnostic vocabulary.
    """

    def __init__(self, source: IBSource):
        self._historical = IBHistoricalSource(source)

    async def fetch(self, tickers):
        daily_end, past_end, today_end = _window_ends()

        # useRTH is locked in the IB adapter (DAILY -> True, intraday
        # -> False), so it's not restated per-call here.
        daily_map, past_map, today_map = await asyncio.gather(
            self._historical.fetch_many(tickers, HistoryWindow(
                bar_size      = BarSize.DAILY,
                lookback_days = 14,
                end           = daily_end,
            )),
            self._historical.fetch_many(tickers, HistoryWindow(
                bar_size      = BarSize.MIN_2,
                lookback_days = 5,
                end           = past_end,
            )),
            self._historical.fetch_many(tickers, HistoryWindow(
                bar_size      = BarSize.MIN_2,
                lookback_days = 1,
                end           = today_end,
            )),
        )
        daily = _transform_bars_dict(daily_map, handle_incoming_dataframe_daily)
        past  = _transform_bars_dict(past_map,  bars_to_avg_volume_frame)
        today = _transform_bars_dict(today_map, bars_to_today_frame)
        return daily, today, past


# =============================================================================
# Polygon implementation
# =============================================================================


class PolygonWarmupSource(WarmupSource):
    """
    Warmup driven by Polygon aggregates REST via the project-local
    ``polygon_client`` / ``polygon_history`` helpers. Opens an aiohttp
    session per fetch batch (short-lived; matches how the existing
    code behaved). Deletes when Polygon moves into
    ``data_sources.polygon._source`` and this file collapses to a
    single generic ``WarmupSource``.
    """

    async def fetch(self, tickers):
        # Local imports keep aiohttp / polygon helpers out of the
        # import graph when this class isn't chosen at runtime.
        from src.helpers                 import polygon_history
        from src.helpers.polygon_client  import PolygonClient

        timeout = aiohttp.ClientTimeout(total=30.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = PolygonClient(
                session  = session,
                api_key  = settings.POLYGON_API_KEY,
                base_url = settings.POLYGON_BASE_URL.rstrip("/"),
            )
            daily_dfs, intraday_pairs = await asyncio.gather(
                asyncio.gather(*(polygon_history.fetch_history_daily(client, t)
                                 for t in tickers)),
                asyncio.gather(*(polygon_history.fetch_intraday_volume_history(client, t)
                                 for t in tickers)),
            )

        def _ok(df) -> bool:
            return df is not None and not df.empty

        daily = {t: d    for t, d in zip(tickers, daily_dfs)      if _ok(d)}
        today = {t: p[0] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[0])}
        past  = {t: p[1] for t, p in zip(tickers, intraday_pairs) if p and _ok(p[1])}
        return daily, today, past


# =============================================================================
# Factory
# =============================================================================


def make_warmup_source(ib_source: Optional[IBSource]) -> WarmupSource:
    """
    Pick the warmup implementation based on ``settings.HISTORY_SOURCE``.

    ``ib_source`` can be ``None`` when ``HISTORY_SOURCE=polygon``.
    Raises if ``HISTORY_SOURCE=ib`` but ``ib_source`` is ``None`` --
    means the caller forgot to build an ``IBSource`` in
    ``initialize_app``.
    """
    if settings.HISTORY_SOURCE == "ib":
        if ib_source is None:
            raise RuntimeError(
                "HISTORY_SOURCE=ib but no IBSource was built. "
                "Check initialize_app's skip-IB condition."
            )
        logger.info("WarmupSource: IB")
        return IBWarmupSource(ib_source)
    if settings.HISTORY_SOURCE == "polygon":
        logger.info("WarmupSource: Polygon")
        return PolygonWarmupSource()
    raise ValueError(f"unknown HISTORY_SOURCE: {settings.HISTORY_SOURCE!r}")
