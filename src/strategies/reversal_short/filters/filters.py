"""
Setup filters for the reversal_short strategy.

Mirror of the reversal_long filters with the sides flipped:

    * ``check_ema9_crossover_down`` -- last 2 candles show an EMA9
      crossover DOWN.
    * ``check_recent_euforia`` -- ``Relatr <= EUFORIC_THRESHOLD`` (the
      env-configured signed threshold, a negative float; see
      ``src.alarms.alarm_logics.detect_euforia``) anywhere in the last
      ``EUFORIA_LOOKBACK_CANDLES`` (default 8).

Both must pass on the same candle for the strategy to fire.
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

from src.alarms.alarm_logics import detect_euforia, is_crossover_down
from src.core.config import settings
from src.database.db_functions import get_last_rows


logger = logging.getLogger(__name__)


# How many of the most recent 2-min candles are inspected for the
# euforia trigger. Symmetric with the long-side capitulation window.
EUFORIA_LOOKBACK_CANDLES: int = 8


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters
# =============================================================================


def check_ema9_crossover_down(df) -> FilterResult:

    label = "EMA9 crossover down"

    tail = df.tail(2)
    passed = bool(is_crossover_down(tail))

    prev, curr = tail.iloc[-2], tail.iloc[-1]
    detail = (
        f"ema9={float(curr['ema9']):.2f}  "
        f"curr={float(curr['close']):.2f}"
    )
    return FilterResult("ema9_down", label, passed, detail)


def check_recent_euforia(
    df, lookback: int = EUFORIA_LOOKBACK_CANDLES,
) -> FilterResult:
    """
    Pass when ``Relatr <= EUFORIC_THRESHOLD`` on any of the last
    ``lookback`` rows in ``df``. ``EUFORIC_THRESHOLD`` is a SIGNED
    (negative) threshold configured in the env; the comparison is a
    straight ``min(relatr) <= threshold`` via ``detect_euforia``.
    """
    threshold = float(settings.EUFORIC_THRESHOLD)
    label = "Check euforia"


    window = df.tail(lookback)
    min_relatr = float(window["relatr"].astype(float).min())
    passed = detect_euforia(window, threshold=threshold)
    return FilterResult(
        id="recent_euf",
        label=label,
        passed=passed,
        detail=f"Recent min relatr = {min_relatr:.2f}",
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
    Run every short-side filter for ``symbol`` and return
    ``(all_passed, results)``. One DB read fetches enough rows for
    both filters.
    """
    df = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream",
        num_rows=EUFORIA_LOOKBACK_CANDLES,
    )

    results: List[FilterResult] = [
        check_recent_euforia(df),
        check_ema9_crossover_down(df),
        
    ]

    return all(r.passed for r in results), results
