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

    label = f"Rvol"

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

    threshold = float(settings.EUFORIC_THRESHOLD)
    label = f"Relatr"

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

    label = f"back to VWAP"

    relatrs = df_all["Relatr"].astype(float).tolist()
    threshold = float(settings.EUFORIC_THRESHOLD)
    idx = _find_last_euforia_index(relatrs, threshold)
    if idx is None:
        return FilterResult("vwap_touch_since_eu", label, False, "no prior euforia detected")

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

    label = "EMA9 crossover"

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
        return "filters PASS (" + "; ".join(f"{r.detail}" for r in results) + ")"
    return "filter FAIL: " + "; ".join(f"{r.detail}" for r in failed)


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
