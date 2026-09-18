"""
Setup filters for the ``two_bar_breakentry`` strategy.

The strategy fires a LONG breakout on the 5-sec bar when price closes
above the max High of the last two 2-min candles AFTER a reversal-long
setup has appeared. This module owns the setup gate: it has to see
recent panic selling before the breakout is worth taking, which is
exactly the same "capitulation" trigger the reversal_long candle
strategy waits for -- shared via ``reversal_shared.detection``.

Only one filter today: ``check_recent_capitulation``. Pass when at
least one of the last ``CAPITULATION_LOOKBACK_CANDLES`` 2-min candles
in ``{symbol}_livestream`` has ``Relatr >= CAPITULATION_THRESHOLD``.

The lookback window is intentionally narrow. "Still capitulating"
(very fresh spike) can still trip the ORB-style breakout on noise, and
"capitulated a long time ago" isn't really a fresh setup either -- the
sweet spot is a capitulation a couple of candles back that has just
started to flatten. Tune ``CAPITULATION_LOOKBACK_CANDLES`` here.

Filter shape (id/label/passed/detail + ``format_summary`` +
``evaluate_filters``) mirrors the ORB and reversal filter modules so
the dashboard viz layer stays generic.
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.strategies.reversal_shared.detection import detect_capitulation

logger = logging.getLogger(__name__)


# How many of the most recent 2-min candles are inspected for the
# capitulation trigger. Narrow by design so the breakout fires shortly
# after the capitulation bar rather than any time in the session with a
# capitulation somewhere in the past.
CAPITULATION_LOOKBACK_CANDLES: int = 3


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters
# =============================================================================


def check_recent_capitulation(
    df, lookback: int = CAPITULATION_LOOKBACK_CANDLES,
) -> FilterResult:
    """
    Pass when ``Relatr >= CAPITULATION_THRESHOLD`` on any of the last
    ``lookback`` rows in ``df``. Delegates to the shared
    ``detect_capitulation`` so the sign / column convention stays in
    one place with the reversal strategy.
    """
    threshold = float(settings.CAPITULATION_THRESHOLD)
    label = "Capitulation check"

    if df.empty:
        return FilterResult(
            id="recent_cap",
            label=label,
            passed=False,
            detail=f"no 2m candles in livestream yet (need last {lookback})",
        )
    if "relatr" not in df.columns:
        return FilterResult(
            id="recent_cap",
            label=label,
            passed=False,
            detail="relatr column missing from livestream",
        )

    window = df.tail(lookback)
    max_relatr = float(window["relatr"].astype(float).max())
    passed = detect_capitulation(window, threshold=threshold)
    return FilterResult(
        id="recent_cap",
        label=label,
        passed=passed,
        detail=f"Recent max relatr = {max_relatr:.2f} (>= {threshold:.2f})",
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


async def evaluate_filters(
    symbol: str, current_price: float,
) -> Tuple[bool, List[FilterResult]]:
    """
    Run every setup filter for ``symbol`` and return
    ``(all_passed, results)``. ``current_price`` is accepted to match
    the realtime-filter signature used elsewhere (ORB long/short) even
    though the only filter today doesn't use it -- keeps the strategy
    orchestrator's call site uniform if a price-gated filter is added.
    """
    df = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream",
        num_rows=CAPITULATION_LOOKBACK_CANDLES,
    )

    results: List[FilterResult] = [
        check_recent_capitulation(df),
    ]

    return all(r.passed for r in results), results
