"""
Startup warmup for shared + per-strategy in-memory state.

At streamer startup we have two historical datasets in memory that
strategies (and their dashboard cards) want to reflect from tick #1:

    * intraday history -- 2-min bars enriched with Rvol/Relatr, ONE
      DataFrame per symbol keyed by SYMBOL. Feeds the shared candle
      timeline only. Both strategies' filter values are computed live
      in their respective ``filters.evaluate_filters`` on every 5-sec
      tick, so nothing else is seeded here.

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

from src.strategies import candle_timeline
from src.strategies.orb_long import state as orb_strategy_state


def warmup_from_intraday(relatr_datasets: dict) -> None:
    """
    Seed the shared candle timeline with each symbol's historical 2-min
    bars. Live 5-sec ticks will animate the next candle after these;
    ``finalize_candle`` keeps appending as the session runs. Both ORB
    and reversal cards render filter rows only, and those are populated
    by the respective strategy on the first 5-sec tick after warmup via
    ``viz.record_filter_results`` -- nothing else to seed here.
    """
    # Upstream validate_tickers guarantees every frame here is non-empty.
    seeded = 0
    for symbol, df in relatr_datasets.items():
        candle_timeline.seed_from_history(symbol, df.to_dict(orient="records"))
        seeded += 1
    logging.debug("Candle timeline seeded (%d symbols)", seeded)


def warmup_from_daily(daily_data: dict) -> None:
    """
    Seed yesterday's RTH high + close per symbol from the daily-bars
    fetch into ORB's strategy state. The dashboard reads yesterday's
    values from there via ``viz.snapshot()`` so we don't keep two copies.

    ``daily_data`` is a ``{symbol: DataFrame}`` mapping produced by
    ``_fetch_history_data`` and narrowed by ``validate_tickers`` -- every
    frame is guaranteed non-empty.
    """
    for symbol, df in daily_data.items():
        yrow = df.iloc[-1]
        orb_strategy_state.record_yesterday_daily(
            symbol=symbol,
            high=float(yrow["high"]),
            close=float(yrow["close"]),
        )
