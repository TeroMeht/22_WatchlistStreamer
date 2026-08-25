
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from src.core.config import settings
from src.helpers.handle_dataframes import (
    bars_to_avg_volume_frame,
    bars_to_today_frame,
    handle_incoming_dataframe_daily,
)
from src.helpers.polygon_client import (
    PolygonClient,
    fetch_daily_bars,
    fetch_intraday_bars,
)


logger = logging.getLogger(__name__)


# Polygon uses ET as its canonical session-date boundary; a daily bar's
# ``t`` is midnight ET of that session. We convert to date in ET so a
# post-market fill after 20:00 ET doesn't accidentally get bucketed into
# the next day.
_ET = ZoneInfo(settings.TIMEZONE)


# ---------------------------------------------------------------------------
# Anchor -- session-end date used to bound both fetches
# ---------------------------------------------------------------------------


def _replay_anchor_date() -> date:
    """
    End-date the warmup fetches should aim at:

      * replay mode -> the CSV's session date.
      * live mode   -> today (wall-clock).

    Local import of ``get_replay_start_datetime`` to keep the live-mode
    import graph free of the replay module.
    """
    if settings.MODE == "replay":
        from src.streamer.replay import get_replay_start_datetime
        return get_replay_start_datetime().date()
    return datetime.now().date()


# ---------------------------------------------------------------------------
# Adapters -- Polygon results[] -> IB-shaped SimpleNamespace
# ---------------------------------------------------------------------------


def _polygon_daily_to_barlike(row: dict) -> SimpleNamespace:
    """
    Shape a Polygon daily aggregate into what
    ``incoming_bars_to_datamodel_format`` expects.

    ``date`` becomes a ``datetime.date`` (matches IB daily bars), computed
    in ET so it's the ET session date -- same date column values the IB
    path would produce.

    ``average`` / ``barCount`` map from Polygon's ``vw`` / ``n``; both
    optional on Polygon too, hence the ``.get``.
    """
    ts_utc = datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc)
    session_date = ts_utc.astimezone(_ET).date()
    return SimpleNamespace(
        date=session_date,
        open=float(row["o"]),
        high=float(row["h"]),
        low=float(row["l"]),
        close=float(row["c"]),
        volume=float(row["v"]),
        average=float(row["vw"]) if row.get("vw") is not None else None,
        barCount=int(row["n"]) if row.get("n") is not None else None,
    )


def _polygon_intraday_to_barlike(row: dict) -> SimpleNamespace:
    """
    Shape a Polygon N-min aggregate into what
    ``incoming_bars_to_datamodel_format`` expects.

    ``date`` is a tz-aware UTC datetime -- ``_intraday_frame`` (in handle_dataframes.py) then does
    ``pd.to_datetime(..., utc=True).dt.tz_convert(TIMEZONE)`` and derives
    the ``Date`` / ``Time`` columns in Helsinki. Same shape IB delivers
    with ``formatDate=2``.
    """
    ts_utc = datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc)
    return SimpleNamespace(
        date=ts_utc,
        open=float(row["o"]),
        high=float(row["h"]),
        low=float(row["l"]),
        close=float(row["c"]),
        volume=float(row["v"]),
        average=float(row["vw"]) if row.get("vw") is not None else None,
        barCount=int(row["n"]) if row.get("n") is not None else None,
    )


# ---------------------------------------------------------------------------
# Public fetches -- signatures parallel to ibclient.py
# ---------------------------------------------------------------------------


# Window sizes -- matched to what the IB versions request today so the
# downstream DataFrames stay the same shape.
#
# * DAILY: IB asks ``durationStr="14 D"`` with ``useRTH=True`` -> up to
#   14 daily bars. Polygon has no "N trading days" primitive, so we ask
#   for a calendar window wide enough to guarantee 14 trading days after
#   weekends/holidays (~21 calendar days is comfortable).
# * INTRADAY: IB asks ``durationStr="5 D"`` at 2-min cadence,
#   ``useRTH=False`` -> ~5 sessions ending at the anchor. Polygon
#   returns all pre/post-market too when we don't restrict, so 6
#   calendar days ending at the anchor covers a full 5-session window
#   even when the anchor lands right after a weekend.
_DAILY_CALENDAR_DAYS = 21
_INTRADAY_CALENDAR_DAYS = 6


async def fetch_history_daily(
    client: PolygonClient,
    symbol: str,
) -> Optional[pd.DataFrame]:
    """
    Polygon-backed replacement for ``ibclient.fetch_history_daily``.

    Ends at ``anchor_date - 1`` (session BEFORE the replay date) so
    today's own daily bar can't leak into ATR -- same anchor rule the
    IB version uses.
    """
    logger.info("[polygon] requesting 14 daily bars for %s", symbol)

    anchor_date = _replay_anchor_date() - timedelta(days=1)
    start_day = anchor_date - timedelta(days=_DAILY_CALENDAR_DAYS)

    try:
        rows = await fetch_daily_bars(client, symbol, start_day, anchor_date)
    except Exception:
        logger.exception("[polygon] daily fetch failed for %s", symbol)
        return None

    if not rows:
        logger.warning("[polygon] no daily data returned for %s", symbol)
        return None

    bars = [_polygon_daily_to_barlike(r) for r in rows]
    atr_df = handle_incoming_dataframe_daily(bars, symbol)
    return atr_df


async def fetch_intraday_volume_history(
    client: PolygonClient,
    symbol: str,
) -> Optional[tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Polygon-backed replacement for ``ibclient.fetch_intraday_volume_history``.

    In replay mode, bars whose timestamp is AT or AFTER the replay
    session-start moment are dropped from the fetched set. Without this
    cutoff Polygon would return the whole replay-day session (Polygon's
    ``/range/N/minute/from/to`` is date-bounded, not time-bounded) --
    those bars then get seeded into the shared candle timeline by
    ``warmup_from_intraday``, and when the live replay finally starts
    dispatching 5-sec bars for the same 2-min intervals, the finalize
    calls just overwrite bars already there. Net effect: the dashboard
    looks static, because every "new" finalized bar was pre-drawn from
    the warmup seed. IB's ``endDateTime=get_replay_start_datetime()``
    achieves the same cutoff on that side; this mirrors it.

    Live mode: no cutoff (replay-start is None), keep everything.
    """
    logger.info("[polygon] requesting 5-day 2-min bars for %s", symbol)

    anchor_date = _replay_anchor_date()
    start_day = anchor_date - timedelta(days=_INTRADAY_CALENDAR_DAYS)

    try:
        rows = await fetch_intraday_bars(
            client, symbol, start_day, anchor_date, minutes=2,
        )
    except Exception:
        logger.exception("[polygon] intraday fetch failed for %s", symbol)
        return None

    if not rows:
        logger.warning("[polygon] no 2-min data returned for %s", symbol)
        return None

    # Replay-only cutoff: drop bars at/after the replay session start.
    if settings.MODE == "replay":
        from src.streamer.replay import get_replay_start_datetime
        # get_replay_start_datetime() returns a tz-aware datetime in
        # settings.TIMEZONE (Helsinki). Compare in UTC ms so the
        # timezone maths line up with Polygon's ``t`` field.
        cutoff_ms = int(get_replay_start_datetime().timestamp() * 1000)
        before = len(rows)
        rows = [r for r in rows if r["t"] < cutoff_ms]
        logger.info(
            "[polygon] %s: replay cutoff dropped %d/%d bars at/after session start",
            symbol, before - len(rows), before,
        )
        if not rows:
            logger.warning(
                "[polygon] %s: all bars fell at/after replay start -- "
                "warmup will have no today_df to seed",
                symbol,
            )
            return None

    # Split the combined window into today vs past based on the
    # Helsinki-local session date, then hand each half to the same
    # helpers the IB three-way fetch uses. The Polygon path stays
    # one HTTP request (cheap) while the downstream shape matches
    # IB's exactly -- ``warmup_source.PolygonWarmupSource`` and
    # ``datastreamer.data_pipe`` don't know which provider fed them.
    from src.streamer.replay import get_effective_today
    today = get_effective_today()
    hki = ZoneInfo(settings.TIMEZONE)

    today_rows, past_rows = [], []
    for r in rows:
        ts_hki = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).astimezone(hki)
        (today_rows if ts_hki.date() == today else past_rows).append(r)

    today_bars = [_polygon_intraday_to_barlike(r) for r in today_rows]
    past_bars  = [_polygon_intraday_to_barlike(r) for r in past_rows]

    today_df = bars_to_today_frame(today_bars, symbol) if today_bars else None
    past_df  = bars_to_avg_volume_frame(past_bars, symbol) if past_bars else None
    if today_df is None or past_df is None:
        logger.warning(
            "[polygon] %s: split produced empty half (today=%s past=%s) -- skip",
            symbol,
            "empty" if today_df is None else f"{len(today_df)} rows",
            "empty" if past_df  is None else f"{len(past_df)} rows",
        )
        return None
    return today_df, past_df
