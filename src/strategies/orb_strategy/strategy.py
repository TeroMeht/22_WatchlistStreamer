"""
ORB long entry strategy -- orchestrator.

Runs on every incoming 5-sec bar. Wires together guards, reference
selection, setup filters, breakout detection, and actions. All logic
lives in sibling modules; this file stays short so the fire path reads
top-to-bottom without hopping into helpers.

Fire semantics (current build):
    * Reference must be available: the 16:32 candle in production, or
      at least two 2-min candles for the test-mode last-2-candles ref.
    * ALL setup filters must pass on the current bar. Only then is
      ``detect_breakout`` consulted -- filters gate the detection step,
      not just the fire decision, so the crossing state doesn't tick
      forward while the setup is disqualified.
    * Fire ONCE per fresh crossing above the reference level; a
      continuing stay above ref does not re-fire.
"""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from ib_async import RealTimeBar

from src.core.config import settings

from .actions.orb_actions import bar_to_candle_row, fire_signal
from .config import ORB_STOP_OFFSET
from .filters.orb_filters import run_all_filters, format_filter_results
from .hooks import orb_hooks as hooks
from .reference import select_reference
from .signals.orb_signals import (
    EDGE_BELOW,
    EDGE_CONTINUATION,
    EDGE_FRESH_CROSSING,
    describe_status,
    detect_breakout,
)

logger = logging.getLogger(__name__)


async def orb_breakout_long(bar: RealTimeBar, symbol: str) -> None:
    """
    Fire a long alarm + entry order when the 5-sec ``bar`` closes above
    the reference candle's Close (16:32 in production, or the most
    recent 2-min candle in test mode) with all setup filters passing.
    """
    bar_time_local = bar.time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )
    today = bar_time_local.date()

    # Chart animates on every tick regardless of what branch we take below.
    hooks.on_bar(symbol, bar_time_local, bar)

    # --- Phase 1: reference selection ---------------------------------------
    # Test mode uses the high/low of the last two 2-min candles;
    # production uses the cached 16:32 candle.
    ref, ref_label = await select_reference(symbol, today)
    if ref is None:
        hooks.on_warming_up(symbol)
        logger.debug("ORB long: %s -- %s reference not available yet "
                     "(LIVE 5s bar %s close=%.2f)",
                     symbol, ref_label, bar_time_local.time(), float(bar.close))
        return
    hooks.on_reference(symbol, ref)

    # --- Phase 2: setup filters (ALL must pass) -----------------------------
    # If any filter fails we neither log at INFO nor advance the breakout
    # detector's state -- the crossing state stays put so the next bar
    # where filters pass can still see a fresh crossing.
    filter_results = await run_all_filters(symbol, float(bar.close))
    
    if not all(r.passed for r in filter_results):
        hooks.on_searching(symbol)
        logger.info(
            "ORB long: %s -- %s (LIVE 5s bar %s close=%.2f | REF close=%.2f)",
            symbol, format_filter_results(filter_results),
            bar_time_local.time(), float(bar.close), ref.ref_close,
        )
        return

    # --- Phase 3: breakout detection ----------------------------------------
    event = detect_breakout(symbol, float(bar.close), ref.ref_close)

    logger.info(
        "ORB long check: %s -- LIVE 5s bar %s close=%.2f | REF 2m candle %s "
        "close=%.2f low=%.2f [%s] | %s -- %s",
        symbol,
        bar_time_local.time(), float(bar.close),
        ref.ref_time, ref.ref_close, ref.ref_low,
        ref_label,
        format_filter_results(filter_results),
        describe_status(event),
    )

    if event.kind == EDGE_BELOW:
        hooks.on_searching(symbol)
        return
    hooks.on_breakout(symbol)
    if event.kind == EDGE_CONTINUATION:
        return
    assert event.kind == EDGE_FRESH_CROSSING  # only path to fire

    # --- Phase 4: fire ------------------------------------------------------
    stop_level = round(ref.ref_low - ORB_STOP_OFFSET, 2)
    logger.info(
        "ORB long breakout FIRED: %s -- LIVE 5s bar %s close=%.2f > "
        "REF 2m candle %s close=%.2f (ref_low=%.2f, stop=%.2f) [%s]",
        symbol,
        bar_time_local.time(), float(bar.close),
        ref.ref_time, ref.ref_close,
        ref.ref_low, stop_level, ref_label,
    )

    candle = bar_to_candle_row(symbol, bar, bar_time_local)
    await fire_signal(candle, stop_level)
    hooks.on_fire(symbol, bar_time_local, float(bar.close), stop_level, ref.ref_close)
