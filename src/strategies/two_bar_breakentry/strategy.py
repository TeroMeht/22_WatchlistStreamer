"""
two_bar_breakentry -- 2-bar breakout entry off a reversal_long setup.

Realtime (5-sec-bar-driven) LONG entry. Sits between the pure candle-
driven ``reversal_long`` (which fires on the 2-min EMA9 crossover after
capitulation) and the ORB breakouts: same 5-sec cadence and same latch
behaviour as ORB long, but the reference isn't the opening range -- it's
the rolling max High of the LAST TWO FINALIZED 2-min candles.

Fire semantics:
    * Setup filter: at least one of the last N 2-min candles has
      ``Relatr >= CAPITULATION_THRESHOLD`` (recent panic selling). Owned
      by ``filters.check_recent_capitulation`` -- same predicate the
      reversal_long candle strategy uses.
    * Reference: rolling max High of the last two 2-min candles
      (``get_reference_from_last_two_candles``). Stop anchor = min Low
      of that same window.
    * Trigger: current 5-sec bar closes STRICTLY ABOVE the reference
      High (``detect_breakout`` with direction="long").
    * One-shot per session per symbol -- once fired, latched until the
      streamer is restarted. Matches ORB behaviour; keeps us from
      re-arming on every 5-sec bar after a fill.

Wire-up mirrors ``orb_long`` deliberately: this file only owns the
per-symbol latch and the phase orchestration; the reference builder,
detection primitive, fire pipeline, and dashboard state all come from
already-shared modules so the code path stays consistent with the
other 5-sec entries.
"""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from data_sources._bar import IncomingBar

from src.core.config import settings
from src.helpers.handle_candles import stream_data_to_candle_row
from src.strategies.actions import fire_signal
from src.strategies.hooks import make_hooks
from src.strategies.orb_shared.breakout_level import (
    get_reference_from_last_two_candles,
)
from src.strategies.orb_shared.detection import detect_breakout

from .filters import filters as long_filters
from .visualization import state as viz

logger = logging.getLogger(__name__)


# Signal name used in the alarm row + Telegram message when this fires.
TWO_BAR_BREAKENTRY_SIGNAL_NAME: str = "two_bar_breakentry"



# =============================================================================
# Fire latch (module-owned; no shared state module needed)
# =============================================================================
import httpx

EXECUTOR_URL = "http://127.0.0.1:8000"      # your backend; move to settings
EXECUTOR_RECEIVE_PATH = "/api/executor/receive_messages"
LIMIT_OFFSET = 0.05                          # limit = trigger + offset (long); adjust to your rule

# symbol -> (trigger, stop) last accepted by the executor
_last_sent: dict[str, tuple[float, float]] = {}


def reset_fired() -> None:
    """Called on streamer restart / warmup so the latch doesn't survive."""
    _fired.clear()
    _last_sent.clear()


async def _send_to_executor(symbol: str, trigger: float, stop: float) -> None:
    """POST the order levels to the executor, but only when they changed."""
    key = symbol.upper()
    levels = (round(trigger, 2), round(stop, 2))
    if _last_sent.get(key) == levels:
        return

    payload = {
        "strategy": TWO_BAR_BREAKENTRY_SIGNAL_NAME,
        "symbol": key,
        "direction": "long",
        "trigger": levels[0],
        "limit": round(levels[0] + LIMIT_OFFSET, 2),
        "stop": levels[1],
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(EXECUTOR_URL + EXECUTOR_RECEIVE_PATH, json=payload)
        r.raise_for_status()
        body = r.json()
    except httpx.ConnectError:
        # Not cached, so the next tick tries again.
        logger.warning("%s: %s -- executor endpoint not available (%s)",
                       TWO_BAR_BREAKENTRY_SIGNAL_NAME, key, EXECUTOR_URL)
        return
    except Exception:
        logger.exception("%s: %s -- executor send failed", TWO_BAR_BREAKENTRY_SIGNAL_NAME, key)
        return

    _last_sent[key] = levels
    logger.info(
        "%s: %s -- sent to executor trigger=%.2f stop=%.2f -> ok=%s status=%s msg=%s",
        TWO_BAR_BREAKENTRY_SIGNAL_NAME, key, levels[0], levels[1],
        body.get("ok"), body.get("status"), body.get("message"),
    )









_fired: dict[str, bool] = {}


def mark_fired(symbol: str) -> None:
    _fired[symbol.upper()] = True


def has_fired(symbol: str) -> bool:
    return _fired.get(symbol.upper(), False)


def reset_fired() -> None:
    """Called on streamer restart / warmup so the latch doesn't survive."""
    _fired.clear()


# =============================================================================
# Hooks -- viz plumbing shared with ORB (chart animation + reference marker)
# =============================================================================


_hooks = make_hooks(viz)


# =============================================================================
# Strategy orchestrator
# =============================================================================


async def two_bar_breakentry(bar: IncomingBar, symbol: str) -> None:
    """
    5-sec-bar-driven 2-bar breakout entry from a reversal_long setup.

    Same call signature as the ORB realtime strategies so it plugs
    directly into ``REALTIME_ENTRY_STRATEGIES`` in the registry with no
    wrapper.
    """
    bar_time_local = bar.date.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo(settings.TIMEZONE)
    )

    # Chart animates on every tick regardless of what branch we take below.
    _hooks.on_bar(symbol, bar_time_local, bar)

    # --- Session one-shot latch -------------------------------------------
    if has_fired(symbol):
        logger.debug(
            "%s: %s already fired this session -- latched until restart "
            "(LIVE 5s bar %s close=%.2f)",
            TWO_BAR_BREAKENTRY_SIGNAL_NAME, symbol, bar_time_local.time(), float(bar.close),
        )
        return

    # --- Phase 1: setup filters ------------------------------------------
    filters_passed, filter_results = await long_filters.evaluate_filters(symbol, float(bar.close))
    viz.record_filter_results(symbol, filter_results)
    filters_summary = long_filters.format_summary(filter_results)


    if filters_passed:
        # --- Phase 2: reference selection ------------------------------------
        breakout_level = get_reference_from_last_two_candles(symbol)
        # --- Phase 5: stop level ---------------------------------------------
        stop_level = round(float(breakout_level.ref_low) - float(settings.ORB_STOP_OFFSET), 2)
        logger.info("Stop level for %s: %.2f (ref low %.2f - offset %.2f)", symbol, stop_level, breakout_level.ref_low, settings.ORB_STOP_OFFSET)

        # tämä täytyy lähettää order executorille
        if breakout_level is None:
            logger.debug(
                "%s: %s -- reference not available yet (need 2 finalized 2m candles) "
                "(LIVE 5s bar %s close=%.2f) | %s",
                TWO_BAR_BREAKENTRY_SIGNAL_NAME, symbol, bar_time_local.time(),
                float(bar.close), filters_summary,
            )
            return

        _hooks.on_reference(symbol, breakout_level)
        _hooks.on_stop(symbol, stop_level)

        # --- Send to executor (only when trigger / stop changed) -------------
        await _send_to_executor(symbol, float(breakout_level.ref_close), stop_level)


        # --- Phase 4: breakout detection -------------------------------------
        # event = detect_breakout(float(bar.close), breakout_level.ref_close, "long")
        # logger.info(
        #     "%s: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
        #     "price=%.2f low=%.2f | %s | %s",
        #     _LOG_PREFIX, symbol,
        #     bar_time_local.time(), float(bar.close),
        #     breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
        #     filters_summary,
        #     event.reason,
        # )
        # if not event.is_breakout:
        #     return



        # --- Phase 6: fire alarm + entry order + viz marker + latch ----------
        # Realtime strategy: map the 5-sec bar to a CandleRow so the shared
        # fire pipeline has the same input shape as candle-driven entries.
        # candle = stream_data_to_candle_row(symbol, bar, bar_time_local)
        # await fire_signal(
        #     candle=candle,
        #     signal_name=TWO_BAR_BREAKENTRY_SIGNAL_NAME,
        #     stop_level=stop_level,
        #     viz=viz,
        #     ref_close=float(breakout_level.ref_close),
        #     live_price=float(bar.close),
        #     mark_fired=mark_fired,
        # )

        # logger.info(
        #     "%s FIRED: %s -- Incoming livestream %s price=%.2f > Breakout level %s "
        #     "price=%.2f (stop=%.2f)",
        #     TWO_BAR_BREAKENTRY_SIGNAL_NAME, symbol,
        #     bar_time_local.time(), float(bar.close),
        #     breakout_level.ref_time, breakout_level.ref_close, stop_level,
        # )

    # --- Phase 3: filter gate --------------------------------------------
    elif not filters_passed:
        logger.debug(
            "%s: %s -- Incoming livestream %s price=%.2f filters=%s",
            TWO_BAR_BREAKENTRY_SIGNAL_NAME, symbol,
            bar_time_local.time(), float(bar.close),
            filters_summary,
        )
        return