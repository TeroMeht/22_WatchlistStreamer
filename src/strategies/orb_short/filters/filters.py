from __future__ import annotations

import logging
from typing import List, NamedTuple, Tuple

import pandas as pd

from src.core.config import settings
from src.database.db_functions import get_last_rows
from src.strategies.orb_shared.yesterday import yesterday_close, yesterday_low

logger = logging.getLogger(__name__)


class FilterResult(NamedTuple):
    id: str
    label: str
    passed: bool
    detail: str


# =============================================================================
# Individual filters (short direction)
# =============================================================================


async def check_rvol_gte(df_last: pd.DataFrame, threshold: float) -> FilterResult:
    """
    Pass when the most recent 2-min candle in ``{symbol}_livestream`` has
    ``Rvol >= threshold``. Rvol is direction-agnostic -- the same volume
    activity is required for a short setup.
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


async def check_price_below_yesterday_low(symbol: str, current_price: float) -> FilterResult:
    """Pass when ``current_price < yesterday's RTH low``."""
    label = "price < yesterday low"
    ylo = yesterday_low(symbol)
    if ylo is None:
        return FilterResult("y_low", label, False, "yesterday low not seeded")
    return FilterResult(
        id="y_low",
        label=label,
        passed=current_price < ylo,
        detail=f"price={current_price:.2f}  vs  ylo={ylo:.2f}",
    )


async def check_price_below_yesterday_close(symbol: str, current_price: float) -> FilterResult:
    """Pass when ``current_price < yesterday's RTH close``."""
    label = "price < yesterday close"
    ycl = yesterday_close(symbol)
    if ycl is None:
        return FilterResult("y_close", label, False, "yesterday close not seeded")
    return FilterResult(
        id="y_close",
        label=label,
        passed=current_price < ycl,
        detail=f"price={current_price:.2f}  vs  yclose={ycl:.2f}",
    )


async def check_premarket_low(df_last: pd.DataFrame, current_price: float) -> FilterResult:
    """
    Pass when ``current_price <= min Low across strictly-premarket 2-min
    candles`` (Time < SESSION_START). Restricting to premarket rows is
    what prevents the "premarket low" from silently becoming the running
    intraday low after the open.
    """
    label = "price <= premarket low"
    if df_last.empty:
        return FilterResult("pm_low", label, False, "no 2m candle in livestream yet")

    pre = df_last[df_last["time"] < settings.SESSION_START]
    if pre.empty:
        return FilterResult("pm_low", label, False, "no premarket 2m candles yet")

    premarket_low = float(pre["low"].min())
    return FilterResult(
        id="pm_low",
        label=label,
        passed=current_price <= premarket_low,
        detail=f"price={current_price:.2f}  vs  pmlo={premarket_low:.2f}",
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
    Run every short-side ORB filter for ``symbol`` at ``current_price``
    and return ``(all_passed, results)``. Result order is the declaration
    order below -- that's the render order on the dashboard.
    """
    df_last = await get_last_rows(table_name=f"{symbol.lower()}_livestream", num_rows=None)

    results: List[FilterResult] = [
        await check_rvol_gte(df_last, settings.RVOL_THRESHOLD),
        await check_price_below_yesterday_low(symbol, current_price),
        await check_price_below_yesterday_close(symbol, current_price),
        await check_premarket_low(df_last, current_price),
    ]

    return all(r.passed for r in results), results
