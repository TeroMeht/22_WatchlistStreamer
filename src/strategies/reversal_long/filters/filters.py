"""
Setup filters for the reversal_long breakout strategy.

Only one filter today: ``check_recent_capitulation``. Passes when at
least one of the last ``lookback`` 2-min candles has
``Relatr >= CAPITULATION_THRESHOLD`` -- i.e. there was recent panic
selling. Once that trigger fires we start monitoring for a 2-bar high
breakout on the 5-sec path.

The lookback window is intentionally narrow so "still capitulating"
(very fresh spike) still blocks a bit of the noise, and "capitulated
then flattening" is the setup we're waiting for.

``FilterResult`` mirrors the ORB filter shape so the dashboard's
dynamic renderer treats reversal rows the same way it treats ORB rows.
See ``orb_long.filters.filters`` for the full field contract; the short
version:

    id      -- stable slug (e.g. ``"recent_cap"``)
    label   -- rendered rule statement, live threshold baked in
               (e.g. ``"recent capitulation (Relatr >= 3.00 in last 3)"``)
    passed  -- bool
    detail  -- right-hand text on the dashboard row, also the log fragment

``evaluate_filters`` returns ``(all_passed, results)``; ``format_summary``
formats the results list for log lines. This module has no side effects
beyond DB reads.
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

from src.core.config import settings
from src.database.db_functions import get_last_rows


logger = logging.getLogger(__name__)


# How many of the most recent 2-min candles are inspected for capitulation.
# Narrow (3) by design so the strategy fires shortly after the capitulation
# bar rather than any time in the session with a capitulation somewhere in
# the past. Tune here if you want a wider or tighter reach.
CAPITULATION_LOOKBACK_CANDLES: int = 3


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters
# =============================================================================


async def check_recent_capitulation(
    symbol: str, lookback: int = CAPITULATION_LOOKBACK_CANDLES,
) -> FilterResult:
    """
    Pass when any of the last ``lookback`` 2-min candles in
    ``{symbol}_livestream`` has ``Relatr >= CAPITULATION_THRESHOLD``.

    Fails when the table has fewer rows than ``lookback``, when the
    Relatr column is missing, or when no candle in the window meets the
    threshold.
    """
    threshold = float(settings.CAPITULATION_THRESHOLD)
    label = f"recent capitulation (Relatr >= {threshold:.2f} in last {lookback})"

    df = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=lookback)
    if df.empty or len(df) < 1:
        return FilterResult("recent_cap", label, False, f"no 2m candles yet (need last {lookback})")
    if "Relatr" not in df.columns:
        return FilterResult("recent_cap", label, False, "Relatr column missing from livestream")

    relatrs = df["Relatr"].astype(float)
    max_relatr = float(relatrs.max())

    return FilterResult(
        id="recent_cap",
        label=label,
        passed=max_relatr >= threshold,
        detail=f"max Relatr = {max_relatr:.2f}",
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
    """
    results: List[FilterResult] = [
        await check_recent_capitulation(symbol),
    ]

    return all(r.passed for r in results), results
