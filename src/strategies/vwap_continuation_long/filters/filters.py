"""
Setup filters for the vwap_continuation_long strategy.

Candle-driven (2-min): each filter reads the finalized rows in
``{symbol}_livestream`` and returns a ``FilterResult`` in the shared
shape used by ORB / reversal_long, so the dashboard's dynamic renderer
treats these rows the same way.

FilterResult contract (id/label/passed/detail): see
``orb_long.filters.filters`` for the full field-by-field description.

Setup story
-----------

We want to enter LONG on the SECOND leg of an upside move, after price
has cooled off. The full journey the filters enforce is:

    1. Earlier in the session, price extended above VWAP hard enough to
       print ``Relatr <= EUFORIC_THRESHOLD`` on at least one 2-min
       candle. That's ``prior_euforia``.

    2. BETWEEN that euforia candle and the current candle, price has
       come back to VWAP. "Back to VWAP" counts if some intermediate
       candle sat inside ``[-VWAP_DISTANCE, +VWAP_DISTANCE]`` OR if
       Relatr flipped from strictly positive to strictly negative
       (price physically crossed UP through VWAP). That's
       ``vwap_touch_since_euforia``.

    3. On the current candle, we have volume: ``Rvol >=
       RVOL_THRESHOLD``. That's ``rvol``.

    4. On the current candle, EMA9 crossover UP: previous Close was
       below the current EMA9 and current Close is above it. Same
       predicate ``helpers.utils.is_crossover_up`` uses elsewhere so
       "crossover" means the same thing across the codebase. That's
       ``ema9_crossover_up`` -- the actual entry trigger.

If all four pass on the same finalized 2-min candle, the strategy fires
the alarm + entry order.

``evaluate_filters`` returns ``(all_passed, results)``; ``format_summary``
formats the results list for log lines. This module has no side effects
beyond DB reads.
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Optional, Tuple

import pandas as pd

from src.alarms.alarm_logics import is_crossover_up
from src.core.config import settings
from src.database.db_functions import get_last_rows


logger = logging.getLogger(__name__)


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters
# =============================================================================


def check_rvol_gte(df_all: pd.DataFrame, threshold: float) -> FilterResult:
    """
    Pass when the most recent 2-min candle has ``Rvol >= threshold``.
    Reads the DataFrame tail rather than ``candle.rvol`` so the same
    fetch feeds every filter here.
    """
    label = f"Rvol >= {threshold:.2f}"
    if df_all.empty or "Rvol" not in df_all.columns:
        return FilterResult("rvol", label, False, "no 2m candle in livestream yet")
    rvol = float(df_all.iloc[-1]["Rvol"])
    return FilterResult(
        id="rvol",
        label=label,
        passed=rvol >= threshold,
        detail=f"Rvol = {rvol:.2f}",
    )


def _find_last_euforia_index(relatrs: List[float], euforic_threshold: float) -> Optional[int]:
    """Index of the most recent candle with ``Relatr <= euforic_threshold``."""
    last = None
    for i, r in enumerate(relatrs):
        if r <= euforic_threshold:
            last = i
    return last


def check_prior_euforia(df_all: pd.DataFrame) -> FilterResult:
    """
    Pass when at least one 2-min candle in the session so far had
    ``Relatr <= EUFORIC_THRESHOLD`` (a sufficiently strong extension
    above VWAP, given ``Relatr = (VWAP - Close) / ATR`` so negative =
    above VWAP).
    """
    threshold = float(settings.EUFORIC_THRESHOLD)
    label = f"prior euforia (Relatr <= {threshold:.2f} in session)"

    if df_all.empty or "Relatr" not in df_all.columns:
        return FilterResult("prior_euforia", label, False, "no Relatr data yet")

    relatrs = df_all["Relatr"].astype(float).tolist()
    min_relatr = min(relatrs)
    return FilterResult(
        id="prior_euforia",
        label=label,
        passed=min_relatr <= threshold,
        detail=f"min Relatr = {min_relatr:+.2f}",
    )


def check_vwap_touch_since_euforia(
    df_all: pd.DataFrame, vwap_distance: float,
) -> FilterResult:
    """
    Pass when, between the LAST euforia candle and the current candle,
    price has actually come back to VWAP.

    Two ways to qualify (either is enough):

        IN-BAND  -- some intermediate candle had
                    ``-vwap_distance <= Relatr <= +vwap_distance``.
                    Direct sit on VWAP.
        CROSS-UP -- some consecutive pair had ``prev > 0`` and
                    ``curr < 0``, i.e. price physically crossed UP
                    through VWAP given the Relatr sign convention.

    Downward crosses (negative -> positive) do NOT count -- those move
    price further below VWAP, opposite of what we want for a long
    continuation.

    Fails cleanly when there was no prior euforia (nothing to anchor
    "since" against). Fails when the euforia is the tail row (no
    subsequent candle yet).
    """
    label = f"back to VWAP since euforia (in +/-{vwap_distance:.2f} OR + to -)"
    if df_all.empty or "Relatr" not in df_all.columns:
        return FilterResult("vwap_touch_since_eu", label, False, "no Relatr data yet")

    relatrs = df_all["Relatr"].astype(float).tolist()
    threshold = float(settings.EUFORIC_THRESHOLD)
    idx = _find_last_euforia_index(relatrs, threshold)
    if idx is None:
        return FilterResult("vwap_touch_since_eu", label, False, "no prior euforia to anchor from")

    since = relatrs[idx:]  # include the euforia candle itself as the anchor
    if len(since) < 2:
        # Euforia just printed on this candle -- no room to have come
        # back to VWAP yet. Show the same "Relatr=X" line the other
        # branches use rather than a wordier explanation.
        return FilterResult(
            "vwap_touch_since_eu", label, False,
            f"Relatr={relatrs[-1]:+.2f}",
        )

    in_band_hits = [r for r in since if -vwap_distance <= r <= vwap_distance]
    cross_up = any(a > 0 and b < 0 for a, b in zip(since, since[1:]))
    current = since[-1]

    passed = bool(in_band_hits) or cross_up
    if cross_up:
        detail = f"cross-up (+ to -), Relatr={current:+.2f}"
    elif in_band_hits:
        detail = f"in-band, Relatr={current:+.2f}"
    else:
        detail = f"Relatr={current:+.2f}"
    return FilterResult(
        id="vwap_touch_since_eu",
        label=label,
        passed=passed,
        detail=detail,
    )


def check_ema9_crossover_up(df_all: pd.DataFrame) -> FilterResult:
    """
    Pass when the last two 2-min candles print an EMA9 crossover UP
    AND the crossover happens above VWAP (current Close > current VWAP).

    Same crossover predicate ``helpers.utils.is_crossover_up`` uses so
    the "crossover" definition stays in lockstep with the reversal
    strategies. The above-VWAP gate is the extra long-continuation
    requirement: an EMA9 flip that prints while price is still below
    VWAP is a different setup and should not fire this strategy.
    """
    label = "EMA9 crossover UP above VWAP (last 2 candles)"
    if df_all.empty:
        return FilterResult("ema9_x_up", label, False, "no 2m candles yet")
    if len(df_all) < 2:
        return FilterResult("ema9_x_up", label, False, "need at least 2 candles")
    for col in ("Close", "EMA9", "VWAP"):
        if col not in df_all.columns:
            return FilterResult("ema9_x_up", label, False, f"{col} column missing")

    tail = df_all.tail(2)
    try:
        crossed = is_crossover_up(tail)
    except Exception as e:
        logger.debug("is_crossover_up raised for ema9 filter: %s", e)
        return FilterResult("ema9_x_up", label, False, "crossover check errored")

    curr = tail.iloc[-1]
    curr_close = float(curr["Close"])
    curr_ema9 = float(curr["EMA9"])
    curr_vwap = float(curr["VWAP"])
    above_vwap = curr_close > curr_vwap

    passed = bool(crossed) and above_vwap
    if not crossed:
        detail = f"Close={curr_close:.2f}, EMA9={curr_ema9:.2f}"
    elif not above_vwap:
        detail = f"crossover but below VWAP (Close={curr_close:.2f}, VWAP={curr_vwap:.2f})"
    else:
        detail = f"crossover above VWAP (Close={curr_close:.2f}, VWAP={curr_vwap:.2f})"
    return FilterResult(
        id="ema9_x_up",
        label=label,
        passed=passed,
        detail=detail,
    )


# =============================================================================
# Aggregate wrapper + log formatter
# =============================================================================


def format_summary(results: List[FilterResult]) -> str:
    """Compact one-line summary for log lines: PASS reasons or per-fail details."""
    failed = [r for r in results if not r.passed]
    if not failed:
        return "filters PASS (" + "; ".join(f"{r.label}: {r.detail}" for r in results) + ")"
    return "filter FAIL: " + "; ".join(f"{r.label} -> {r.detail}" for r in failed)


async def evaluate_filters(symbol: str) -> Tuple[bool, List[FilterResult]]:
    """
    Run every filter for ``symbol`` and return ``(all_passed, results)``.
    ``results`` preserves the order the filters are declared in below --
    that's the render order on the dashboard.

    One DB fetch (all session rows for the livestream table) feeds every
    filter. The current 2-min candle is already inserted by the time
    ``run_strategies`` reaches us (see ``finalize_candle`` in
    ``process_incoming_data``), so the tail row IS the candle we're
    evaluating on.
    """
    df_all = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=None)

    results: List[FilterResult] = [
        check_rvol_gte(df_all, settings.RVOL_THRESHOLD),
        check_prior_euforia(df_all),
        check_vwap_touch_since_euforia(df_all, settings.VWAP_DISTANCE),
        check_ema9_crossover_up(df_all),
    ]

    return all(r.passed for r in results), results
