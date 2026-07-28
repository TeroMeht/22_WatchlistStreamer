"""
Setup filters for the reversal_long breakout strategy.

Only one filter today: ``check_recent_capitulation``. Fires the strategy
gate as PASS when at least one of the last ``lookback`` 2-min candles
has ``Relatr >= CAPITULATION_THRESHOLD`` -- i.e. there was recent panic
selling. Once that trigger fires we start monitoring for a 2-bar high
breakout on the 5-sec path.

The lookback window is intentionally narrow so "still capitulating"
(very fresh spike) still blocks a bit of the noise, and "capitulated
then flattening" is the setup we're waiting for.
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


async def check_recent_capitulation(symbol: str, lookback: int = CAPITULATION_LOOKBACK_CANDLES) -> FilterResult:
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


async def run_all_filters(symbol: str) -> List[FilterResult]:
    return [
        await check_recent_capitulation(symbol),
    ]


def format_filter_results(results: List[FilterResult]) -> str:
    """Compact one-line summary useful for log lines: PASS or per-fail reasons."""
    failed = [r for r in results if not r.passed]
    if not failed:
        return "filters PASS (" + "; ".join(r.reason for r in results) + ")"
    return "filter FAIL: " + "; ".join(r.reason for r in failed)


async def evaluate_filters(
    symbol: str,
    current_price: float,
    breakout_level,
    bar_time_local,
) -> Tuple[bool, str]:
    """
    Run all setup filters and handle the miss-log path here so callers
    don't have to branch. Returns ``(passed, summary)``:

      * ``passed``  -- ``True`` when every filter passed, else ``False``.
      * ``summary`` -- compact one-line string built by
        ``format_filter_results`` for downstream log splicing.

    On a miss, logs a one-line summary here in the same shape as the
    pass-case Phase 3 log so both paths read the same left-to-right.
    """
    filter_results = await run_all_filters(symbol)
    summary = format_filter_results(filter_results)
    passed = all(r.passed for r in filter_results)
    if not passed:
        logger.info(
            "reversal_long: %s -- Incoming livestream %s price=%.2f | Breakout level %s "
            "price=%.2f low=%.2f | %s",
            symbol,
            bar_time_local.time(), current_price,
            breakout_level.ref_time, breakout_level.ref_close, breakout_level.ref_low,
            summary,
        )
    return passed, summary
