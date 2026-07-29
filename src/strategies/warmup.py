"""
Startup warmup for shared + per-strategy in-memory state.

At streamer startup we have two historical datasets in memory that
strategies (and their dashboard cards) want to reflect from tick #1:

    * intraday history -- 2-min bars enriched with Rvol/Relatr, ONE
      DataFrame per symbol keyed by SYMBOL. Feeds the shared candle
      timeline plus each strategy's per-symbol dashboard metric (ORB's
      latest Rvol, reversal's recent max Relatr).

    * daily history -- one DataFrame per symbol with the last 14 daily
      bars ending yesterday (useRTH=True). The LAST row is yesterday's
      regular-hours session -- premarket is naturally excluded. Feeds
      ORB's yesterday-level filter state.

Rather than having the streamer know how each strategy consumes the
history (which columns, which lookback, which sink), we do the loop
here once and delegate strategy-specific bits to their own modules.
The streamer just imports this and calls ``warmup_from_intraday`` +
``warmup_from_daily`` after the indicator-calculation phase.
"""

from __future__ import annotations

import logging
from typing import Iterable

from src.strategies import candle_timeline
from src.strategies.orb_long import state as orb_strategy_state
from src.strategies.orb_long.visualization import state as orb_viz
from src.strategies.reversal_long.visualization import state as reversal_viz


def warmup_from_intraday(relatr_datasets: dict) -> None:
    """
    Seed the shared candle timeline with each symbol's historical 2-min
    bars and warm up per-strategy dashboard metrics from the same frame.
    Live 5-sec ticks will animate the next candle after these;
    ``finalize_candle`` keeps appending as the session runs.

    Per-strategy warmup:
        * ORB -- last row's Rvol so the "Rvol > 3" check has a value on
          load.
        * reversal_long -- tail of the historical Relatr column so the
          "recent capitulation" check reflects state at startup. The
          writer maintains a rolling window internally.
    """
    seeded = 0
    for symbol, df in relatr_datasets.items():
        if df is None or df.empty:
            continue
        candle_timeline.seed_from_history(symbol, df.to_dict(orient="records"))
        if "Rvol" in df.columns:
            orb_viz.record_rvol(symbol, float(df.iloc[-1]["Rvol"]))
        if "Relatr" in df.columns:
            for r in df["Relatr"].dropna().tail(reversal_viz.RECENT_RELATR_WINDOW):
                reversal_viz.record_relatr(symbol, float(r))
        seeded += 1
    logging.debug(
        "Candle timeline seeded (%d symbols) + per-strategy overlays warmed up",
        seeded,
    )


def warmup_from_daily(daily_data: Iterable) -> None:
    """
    Seed yesterday's RTH high + close per symbol from the daily-bars
    fetch into ORB's strategy state. The dashboard reads yesterday's
    values from there via ``viz.snapshot()`` so we don't keep two copies.
    """
    for df in daily_data:
        if df is None or df.empty:
            continue
        symbol = df["Symbol"].iloc[0]
        yrow = df.iloc[-1]
        orb_strategy_state.record_yesterday_daily(
            symbol=symbol,
            high=float(yrow["High"]),
            close=float(yrow["Close"]),
        )
