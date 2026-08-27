"""
Setup filters for the reversal_long strategy.

Two filters, both evaluated on every finalized 2-min candle:

    * ``check_ema9_crossover_up`` -- last 2 candles show an EMA9
      crossover UP (prev close below EMA9, current close above EMA9).
      Delegates to the shared ``is_crossover_up`` in
      ``src.helpers.utils`` / ``src.alarms.alarm_logics``.
    * ``check_recent_capitulation`` -- ``Relatr >= CAPITULATION_THRESHOLD``
      anywhere in the last ``CAPITULATION_LOOKBACK_CANDLES`` (default 8).

Both must pass on the same candle for the strategy to fire (the
strategy AND-gates them via ``all(r.passed for r in results)``).

``FilterResult`` mirrors the ORB / vwap_continuation_long filter shape
so the dashboard's dynamic renderer treats reversal_long rows the same
way. See ``orb_long.filters.filters`` for the field contract.

``evaluate_filters`` returns ``(all_passed, results)``; ``format_summary``
formats the results list for log lines. This module has no side effects
beyond the DB read.
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

from src.alarms.alarm_logics import detect_capitulation, is_crossover_up
from src.core.config import settings
from src.database.db_functions import get_last_rows


logger = logging.getLogger(__name__)


# How many of the most recent 2-min candles are inspected for the
# capitulation trigger. Wider than the ORB / VWAP filters because a
# capitulation from ~15 minutes ago is still a valid "aftermath" setup.
CAPITULATION_LOOKBACK_CANDLES: int = 8


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters
# =============================================================================


def check_ema9_crossover_up(df) -> FilterResult:
    """
    Pass when the last two finalized 2-min candles show an EMA9
    crossover UP (prev close < curr ema9 AND curr close > curr ema9).
    Fails safely when fewer than 2 rows are available.
    """
    label = "EMA9 crossover up"


    tail = df.tail(2)
    passed = bool(is_crossover_up(tail))

    prev, curr = tail.iloc[-2], tail.iloc[-1]
    detail = (
        f"ema9={float(curr['ema9']):.2f}  "
        f"curr={float(curr['close']):.2f}"
    )
    return FilterResult("ema9_up", label, passed, detail)


def check_recent_capitulation(
    df, lookback: int = CAPITULATION_LOOKBACK_CANDLES,
) -> FilterResult:
    """
    Pass when ``Relatr >= CAPITULATION_THRESHOLD`` on any of the last
    ``lookback`` rows in ``df``. Delegates to the shared
    ``detect_capitulation`` so the sign / column convention stays in
    one place.
    """
    threshold = float(settings.CAPITULATION_THRESHOLD)
    label = "Capitulation check"

    window = df.tail(lookback)
    max_relatr = float(window["relatr"].astype(float).max())
    passed = detect_capitulation(window, threshold=threshold)
    return FilterResult(
        id="recent_cap",
        label=label,
        passed=passed,
        detail=f"Recent max relatr = {max_relatr:.2f}"
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
    Run every long-side filter for ``symbol`` and return
    ``(all_passed, results)``. One DB read fetches enough rows for BOTH
    filters -- the wider capitulation window is the upper bound.
    """
    df = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream",
        num_rows=CAPITULATION_LOOKBACK_CANDLES,
    )

    results: List[FilterResult] = [
        check_recent_capitulation(df),
        check_ema9_crossover_up(df),
        
    ]

    return all(r.passed for r in results), results
