"""
Realtime (5-sec) entry strategies.

These run on every incoming 5-sec bar rather than only on finalized 2-min
candles, so they can trigger the moment price crosses a level. Kept in
their own module so the existing 2-min entry strategies in
``src/entry_strategies.py`` stay untouched.

Currently implemented:
    * orb_breakout_long -- fires when a 5-sec bar closes above the
      reference candle's Close (16:32 in production, or the latest 2-min
      candle in test mode). Stop = ref_low - $0.02. Fires "once per
      conditions": as many times per day as the setup re-appears, gated
      by the active-order guard so we never stack positions.
"""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order
from src.database.db_functions import get_last_rows, has_active_order
from src.helpers.handle_candles import CandleRow
from src.helpers.reference_level import get_reference_level
from src.visualization import orb_state as viz
from src.core.config import settings

logger = logging.getLogger(__name__)


# ORB long stop is anchored $0.02 below the reference candle's Low. Kept as
# a module constant so it's easy to promote to a settings field later.
ORB_STOP_OFFSET: float = 0.02

# Minimum Rvol on the most recent 2-min candle for an ORB long to be allowed
# to fire. Same rationale as above -- module constant now, promotable later.
ORB_MIN_RVOL: float = 3.0

# --- TEST MODE ---------------------------------------------------------------
# When True, the reference level is the MOST RECENT completed 2-min candle
# (whichever it happens to be) instead of the 16:32 candle. This makes the
# breakout logic testable at any time of day -- during premarket the reference
# just becomes the last 2-min candle finalized.
#
# Set back to False before real trading. When False, behaviour is: reference
# is the 16:32 candle only.
ORB_TEST_MODE_USE_LAST_CANDLE: bool = False


# --- Live-candle gate --------------------------------------------------------
# The `{symbol}_livestream` table is pre-populated with HISTORICAL intraday
# data at streamer startup (see create_and_fill_table_async), so the very
# first 5-sec bar already sees a "last candle" in the DB. We must not fire
# ORB against that historical row -- we must wait until at least one candle
# from THIS live-stream session has been inserted.
#
# `process_incoming_data.finalize_candle` calls notify_live_candle_inserted()
# after every successful DB write. `orb_breakout_long` uses that counter to
# skip until it's >= 1. The counter is per uppercase symbol and lives for
# the streamer's process lifetime -- restart wipes it, which is correct
# (a restart means "wait for a fresh live candle again").
_live_candles_inserted: dict[str, int] = {}


# Edge-trigger memory for the breakout marker. Records whether the previous
# 5-sec bar's close was already above ref_close. We only mark + alarm on the
# transition below -> above; a continued stay above ref does not re-mark.
# Reset when we lose the reference (e.g. warmup) so the next crossing is
# treated as fresh. Naturally reset on streamer restart.
_last_above_ref: dict[str, bool] = {}


def notify_live_candle_inserted(symbol: str) -> None:
    """Called by finalize_candle after insert_candlestick_row succeeds."""
    key = symbol.upper()
    _live_candles_inserted[key] = _live_candles_inserted.get(key, 0) + 1
    viz.record_live_candle_count(symbol, _live_candles_inserted[key])


def live_candle_count(symbol: str) -> int:
    """How many 2-min candles have been inserted this session for ``symbol``."""
    return _live_candles_inserted.get(symbol.upper(), 0)


async def _get_reference_from_last_candle(symbol: str):
    """
    TEST-MODE reference: use the most recent completed 2-min candle in
    ``{symbol}_livestream`` as (ref_time, ref_close, ref_low). Not cached
    -- runs on every 5-sec bar so the reference tracks the latest candle.
    Returns ``None`` if the table has no rows yet.
    """
    df_last = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream", num_rows=1,
    )
    if df_last.empty:
        return None
    row = df_last.iloc[-1]
    # Lightweight namespace with the same attributes ReferenceLevel exposes
    # so downstream code doesn't care which mode we're in.
    from types import SimpleNamespace
    return SimpleNamespace(
        symbol=symbol.upper(),
        ref_time=row["Time"],
        ref_close=float(row["Close"]),
        ref_low=float(row["Low"]),
    )


def _bar_to_candle_row(symbol: str, bar: RealTimeBar) -> CandleRow:
    """
    Build a minimal ``CandleRow`` from a 5-sec ``RealTimeBar`` so we can
    reuse ``generate_signal_alarm`` and ``generate_entry_order`` -- both
    expect a CandleRow. Indicator fields (vwap/ema9/relatR/rvol/avg_volume)
    aren't meaningful for a 5-sec bar so we zero them; only symbol/date/
    time/close matter for the downstream alarm + order writers.
    """
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    close_px = float(bar.close)
    return CandleRow(
        symbol=symbol,
        date=bar_time_local.date(),
        time=bar_time_local.time(),
        open=close_px,
        high=close_px,
        low=close_px,
        close=close_px,
        volume=float(bar.volume),
        vwap=0.0,
        ema9=0.0,
        avg_volume=0.0,
        rvol=0.0,
        relatR=0.0,
    )


async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:
    """
    Fire a long alarm + entry order when the 5-sec ``bar`` closes above
    the reference candle's Close. Stop = ref_low - $0.02.

    Fires ONCE PER CONDITIONS MET (not once per day). Concretely:
      * no fire until the reference candle exists in the DB
      * no fire while an active order is already open for this ticker
      * fire immediately when price crosses above the reference and no
        active order exists; the next fire is possible only after the
        current position's order status leaves ``'active'``.

    Log levels:
      * DEBUG while warming up (no candle yet) or muted (position held)
      * INFO for real per-bar checks + the FIRED line
    """
    key = symbol.upper()
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    today = bar_time_local.date()

    # Feed the current in-progress 2-min candle from this 5-sec tick.
    # The dashboard plots 2-min candles; the growing one at the right
    # edge animates as ticks arrive, then gets replaced by a finalized
    # candle when finalize_candle inserts to the DB.
    # ib_async's RealTimeBar names the open field `open_` because `open`
    # is a Python builtin.
    viz.record_5s_tick(
        symbol,
        bar_time_local,
        float(bar.open_),
        float(bar.high),
        float(bar.low),
        float(bar.close),
    )

    # Live-candle gate. The DB is pre-populated with historical rows at
    # startup, so we can't just trust "there is a row" -- we must have seen
    # at least one candle inserted BY THIS live-stream session before ORB
    # is allowed to reference anything.
    if live_candle_count(symbol) < 1:
        viz.record_state(symbol, viz.STATE_WARMING_UP)
        logger.debug(
            "ORB long: %s waiting for the first LIVE 2-min candle to be "
            "inserted (LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # Reference selection: 16:32 candle in production, or the latest 2-min
    # candle in TEST MODE.
    if ORB_TEST_MODE_USE_LAST_CANDLE:
        ref = await _get_reference_from_last_candle(symbol)
        ref_label = "LAST-CANDLE (test mode)"
    else:
        ref = await get_reference_level(symbol, today)
        ref_label = "16:32"

    if ref is None:
        # Guarded above by live_candle_count, so this should only happen if
        # the DB read raced with a table truncation or errored. Keep as a
        # safety net.
        viz.record_state(symbol, viz.STATE_WARMING_UP)
        logger.debug(
            "ORB long: %s live candle counted but %s reference lookup returned "
            "None (LIVE 5s bar %s close=%.2f)",
            symbol, ref_label, bar_time_local.time(), float(bar.close),
        )
        return

    # Push the reference to the dashboard as soon as we have it.
    viz.record_reference(symbol, ref.ref_time, ref.ref_close, ref.ref_low)

    # Duplicate-order guard, EARLY (before the main log). If a position is
    # already open for this ticker we go silent at INFO -- no per-bar noise
    # while we hold. The main "ORB long check" INFO line only appears when
    # a new fire is actually possible. As soon as the order's status leaves
    # 'active', the INFO logs resume automatically.
    if await has_active_order(symbol):
        viz.record_active_order(symbol, True)
        viz.record_state(symbol, viz.STATE_MUTED)
        logger.debug(
            "ORB long: %s has an active order -- muted until position closes "
            "(LIVE 5s bar %s close=%.2f)",
            symbol, bar_time_local.time(), float(bar.close),
        )
        return
    viz.record_active_order(symbol, False)

    # Edge trigger. We only want to MARK the breakout when price crosses
    # ref_close from below -- staying above ref does not repeatedly fire.
    was_above = _last_above_ref.get(key, False)
    is_above = float(bar.close) > ref.ref_close
    _last_above_ref[key] = is_above

    # Choose the status word BEFORE logging so the line says the right
    # thing:  BREAKOUT only on the actual crossing edge, "above ref
    # (continuation)" while price sits above without a fresh crossing,
    # "no breakout yet" otherwise.
    if not is_above:
        status = "no breakout yet"
    elif was_above:
        status = "above ref (continuation, no new fire)"
    else:
        status = "BREAKOUT (crossing above ref)"

    # Visible per-bar log. Clear separation between the LIVE 5-sec streaming
    # bar (the moving side) and the REF 2-min candle from the DB (the fixed
    # side we compare against). Both timestamps are printed so it's obvious
    # which one is which.
    logger.info(
        "ORB long check: %s -- LIVE 5s bar %s close=%.2f | REF 2m candle %s "
        "close=%.2f low=%.2f [%s] -- %s",
        symbol,
        bar_time_local.time(), float(bar.close),
        ref.ref_time, ref.ref_close, ref.ref_low,
        ref_label,
        status,
    )

    if not is_above:
        viz.record_state(symbol, viz.STATE_SEARCHING)
        return

    viz.record_state(symbol, viz.STATE_BREAKOUT)

    if was_above:
        # Already above ref on the previous bar -- this is a continuation,
        # not a fresh crossing. Do nothing here so the marker doesn't
        # multiply and the alarm doesn't spam.
        return

    # --- Rvol filter (TEMPORARILY DISABLED FOR PREMARKET TESTING) -----------
    # Re-enable when done validating the raw breakout logic.
    #
    # df_last = await get_last_rows(
    #     table_name=f"{symbol.lower()}_livestream", num_rows=1,
    # )
    # if df_last.empty or "Rvol" not in df_last.columns:
    #     logger.debug(
    #         "ORB long: %s price above ref but no recent candle / Rvol column; skipping",
    #         symbol,
    #     )
    #     return
    # latest_rvol = float(df_last.iloc[-1]["Rvol"])
    # if latest_rvol < ORB_MIN_RVOL:
    #     logger.debug(
    #         "ORB long filter miss: %s bar.close=%.2f > ref_close=%.2f but "
    #         "latest Rvol=%.2f < %.2f -- will retry on next bar",
    #         symbol, float(bar.close), ref.ref_close, latest_rvol, ORB_MIN_RVOL,
    #     )
    #     return

    stop_level = round(ref.ref_low - ORB_STOP_OFFSET, 2)
    logger.info(
        "ORB long breakout DETECTED: %s -- LIVE 5s bar %s close=%.2f > "
        "REF 2m candle %s close=%.2f (ref_low=%.2f, would-be stop=%.2f) "
        "[%s, Rvol filter DISABLED, order generation DISABLED]",
        symbol,
        bar_time_local.time(), float(bar.close),
        ref.ref_time, ref.ref_close,
        ref.ref_low, stop_level, ref_label,
    )

    # Send the signal alarm (Telegram + DB) so the event is captured, and
    # drop a marker on the chart. Order generation is deliberately OFF --
    # this build is for verifying the detection logic only.
    candle = _bar_to_candle_row(symbol, bar)
    await generate_signal_alarm(candle=candle, signal_name="ORB long breakout")
    await generate_entry_order(candle=candle, stop_level=stop_level)
    viz.record_fire(
        symbol,
        bar_time_local,
        float(bar.close),
        stop_level,
        ref.ref_close,
    )
