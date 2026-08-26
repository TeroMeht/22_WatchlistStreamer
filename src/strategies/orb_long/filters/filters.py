"""
ORB long entry-strategy filters.

Each filter is a small async function that returns a ``FilterResult``:
an ``(id, label, passed, detail)`` tuple. Filters are pure -- they only
read data (DB or in-memory state), never write, never touch alarms/orders,
never touch the visualization state.

One wrapper -- ``evaluate_filters`` -- runs every filter, aggregates the
results, and returns ``(all_passed, results)`` to the strategy. The
strategy logs the outcome (via ``format_summary``) and hands the results
list to the viz layer for dashboard rendering; this module has no side
effects beyond DB reads.

Long-side gates (all must pass):
    * price >= premarket high
    * Rvol  >= settings.RVOL_THRESHOLD
    * price >  yesterday's RTH high
    * price >  yesterday's RTH close

``FilterResult`` fields:
    id      -- stable identifier for the filter (e.g. ``"rvol"``). The
               dashboard uses this to keep a row across polls, so it
               must not change once shipped.
    label   -- human-readable rule statement WITH the live threshold
               baked in (e.g. ``"Rvol >= 1.50"``).
    passed  -- did the filter pass on this evaluation?
    detail  -- right-hand text on the dashboard row and the log fragment.

Signature contract for every filter:
    async def check_<name>(...) -> FilterResult
        passed=True  -> allowed to proceed
        passed=False -> strategy short-circuits; ``detail`` is safe to
                        include in a log line
"""

from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

import pandas as pd

from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.strategies.orb_shared.yesterday import yesterday_close, yesterday_high

logger = logging.getLogger(__name__)


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters (long direction)
# =============================================================================


async def check_rvol_gte(df_last: pd.DataFrame, threshold: float) -> FilterResult:
    """
    Pass when the most recent 2-min candle in ``{symbol}_livestream`` has
    ``Rvol >= threshold``.
    """
    label = f"Rvol >= {threshold:.2f}"
    if df_last.empty:
        return FilterResult("rvol", label, False, "no 2m candle in livestream yet")
    rvol = float(df_last.iloc[-1]["rvol"])
    return FilterResult(
        id="rvol",
        label=label,
        passed=rvol >= threshold,
        detail=f"Rvol = {rvol:.2f}",
    )


async def check_price_above_yesterday_high(symbol: str, current_price: float) -> FilterResult:
    """Pass when ``current_price > yesterday's RTH high``."""
    label = "price > yesterday high"
    yhi = yesterday_high(symbol)
    if yhi is None:
        return FilterResult("y_high", label, False, "yesterday high not seeded")
    return FilterResult(
        id="y_high",
        label=label,
        passed=current_price > yhi,
        detail=f"price={current_price:.2f}  vs  yhi={yhi:.2f}",
    )


async def check_price_above_yesterday_close(symbol: str, current_price: float) -> FilterResult:
    """Pass when ``current_price > yesterday's RTH close``."""
    label = "price > yesterday close"
    ycl = yesterday_close(symbol)
    if ycl is None:
        return FilterResult("y_close", label, False, "yesterday close not seeded")
    return FilterResult(
        id="y_close",
        label=label,
        passed=current_price > ycl,
        detail=f"price={current_price:.2f}  vs  yclose={ycl:.2f}",
    )


async def check_premarket_high(df_last: pd.DataFrame, current_price: float) -> FilterResult:
    """
    Pass when ``current_price >= max High across strictly-premarket 2-min
    candles`` (Time < SESSION_START). Restricting to premarket rows is
    what prevents the "premarket high" from silently becoming the running
    intraday high after the open.
    """
    label = "price >= premarket high"
    if df_last.empty:
        return FilterResult("pm_high", label, False, "no 2m candle in livestream yet")

    pre = df_last[df_last["time"] < settings.SESSION_START]
    if pre.empty:
        return FilterResult("pm_high", label, False, "no premarket 2m candles yet")

    premarket_high = float(pre["high"].max())
    return FilterResult(
        id="pm_high",
        label=label,
        passed=current_price >= premarket_high,
        detail=f"price={current_price:.2f}  vs  pmhi={premarket_high:.2f}",
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


async def evaluate_filters(symbol: str, current_price: float) -> Tuple[bool, List[FilterResult]]:
    """
    Run every long-side ORB filter for ``symbol`` at ``current_price``
    and return ``(all_passed, results)``. Result order is the declaration
    order below -- that's the render order on the dashboard.
    """
    df_last = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=None)

    results: List[FilterResult] = [
        await check_rvol_gte(df_last, settings.RVOL_THRESHOLD),
        await check_price_above_yesterday_high(symbol, current_price),
        await check_price_above_yesterday_close(symbol, current_price),
        await check_premarket_high(df_last, current_price),
    ]

    return all(r.passed for r in results), results
