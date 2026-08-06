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

One wrapper -- ``evaluate_filters`` -- runs every filter, aggregates
the results, and returns ``(passed, summary)`` to the strategy. The
strategy logs the outcome and decides whether to short-circuit; this
module has no side effects beyond DB reads.
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
    passed: bool
    reason: str


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
    df = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=lookback)
    if df.empty or len(df) < 1:
        return FilterResult(False, f"no 2m candles in livestream table (need last {lookback})")
    if "Relatr" not in df.columns:
        return FilterResult(False, "Relatr column missing from livestream table")

    threshold = settings.CAPITULATION_THRESHOLD
    relatrs = df["Relatr"].astype(float)
    max_relatr = float(relatrs.max())

    if max_relatr < threshold:
        return FilterResult(
            False,
            f"no recent capitulation (max Relatr in last {lookback}={max_relatr:.2f} < {threshold:.2f})",
        )
    return FilterResult(
        True,
        f"capitulation seen (max Relatr in last {lookback}={max_relatr:.2f} >= {threshold:.2f})",
    )


# =============================================================================
# Aggregate wrapper
# =============================================================================


def _format(results: List[FilterResult]) -> str:
    """Compact one-line summary for log lines: PASS reasons or per-fail reasons."""
    failed = [r for r in results if not r.passed]
    if not failed:
        return "filters PASS (" + "; ".join(r.reason for r in results) + ")"
    return "filter FAIL: " + "; ".join(r.reason for r in failed)


async def evaluate_filters(symbol: str) -> Tuple[bool, str]:

    results = [
        await check_recent_capitulation(symbol),
    ]
    
    return all(r.passed for r in results), _format(results)
