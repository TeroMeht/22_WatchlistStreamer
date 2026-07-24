"""
ORB entry-strategy filters.

Each filter is a small async function that returns a ``FilterResult``:
a (passed, reason) pair. Filters are pure -- they only read data (DB or
in-memory state), never write, never touch alarms/orders, never touch
the visualization state. Composition happens in the strategy layer.

Signature contract for every filter:
    async def check_<name>(symbol: str, ...) -> FilterResult
        passed=True  -> strategy allowed to proceed
        passed=False -> strategy should skip; ``reason`` explains why
                        and is safe to include in a log line
"""

from __future__ import annotations

import logging
from typing import NamedTuple
from typing import List
from ..config import ORB_MIN_RVOL
from src.database.db_functions import get_last_rows

from ..state import yesterday_close, yesterday_high

logger = logging.getLogger(__name__)


class FilterResult(NamedTuple):
    passed: bool
    reason: str


async def check_rvol_gte(symbol: str, threshold: float) -> FilterResult:
    """
    Pass when the most recent 2-min candle in ``{symbol}_livestream`` has
    ``Rvol >= threshold``. Uses the latest row available -- historical
    or live-finalized, whichever is most recent.

    Fails (with a descriptive reason) when Rvol isn't yet available, the
    column is missing, or the value is below threshold.
    """
    df_last = await get_last_rows(
        table_name=f"{symbol.lower()}_livestream", num_rows=1,
    )
    if df_last.empty:
        return FilterResult(False, "no 2m candle in livestream table yet")
    if "Rvol" not in df_last.columns:
        return FilterResult(False, "Rvol column missing from livestream table")
    rvol = float(df_last.iloc[-1]["Rvol"])
    if rvol < threshold:
        return FilterResult(False, f"Rvol={rvol:.2f} < {threshold:.2f}")
    return FilterResult(True, f"Rvol={rvol:.2f} >= {threshold:.2f}")


async def check_price_above_yesterday_high(symbol: str, current_price: float) -> FilterResult:
    """
    Pass when ``current_price > yesterday's RTH high``. Yesterday's high is
    seeded once at streamer startup from the useRTH=True daily fetch, so
    premarket is naturally excluded. Fails if the seed hasn't happened
    yet (first-run race) or price is at/below yesterday's high.
    """
    yhi = yesterday_high(symbol)
    if yhi is None:
        return FilterResult(False, "yesterday high not seeded")
    if current_price <= yhi:
        return FilterResult(False, f"price={current_price:.2f} <= yhi={yhi:.2f}")
    return FilterResult(True, f"price={current_price:.2f} > yhi={yhi:.2f}")


async def check_price_above_yesterday_close(symbol: str, current_price: float) -> FilterResult:
    """
    Pass when ``current_price > yesterday's RTH close``. Same seeding
    contract as ``check_price_above_yesterday_high``.
    """
    ycl = yesterday_close(symbol)
    if ycl is None:
        return FilterResult(False, "yesterday close not seeded")
    if current_price <= ycl:
        return FilterResult(False, f"price={current_price:.2f} <= yclose={ycl:.2f}")
    return FilterResult(True, f"price={current_price:.2f} > yclose={ycl:.2f}")


async def run_all_filters(symbol: str, current_price: float) -> List[FilterResult]:
     return [
    #     await check_rvol_gte(symbol, ORB_MIN_RVOL),
    #     await check_price_above_yesterday_high(symbol, current_price),
    #     await check_price_above_yesterday_close(symbol, current_price),
    ]


def format_filter_results(results: List[FilterResult]) -> str:
    """Compact one-line summary useful for log lines: PASS or per-fail reasons."""
    failed = [r for r in results if not r.passed]
    if not failed:
        return "filters PASS (" + "; ".join(r.reason for r in results) + ")"
    return "filter miss: " + "; ".join(r.reason for r in failed)
