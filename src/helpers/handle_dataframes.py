"""
Warmup-side DataFrame transforms used by the IB three-way fetch path.

Four public transforms + the seed-and-enrich walk:

  * ``bars_to_today_frame``          -- today's raw OHLCV per symbol
                                        (used by seed_and_enrich_intraday)
  * ``bars_to_avg_volume_frame``     -- past N sessions -> winsorized
                                        per-slot Avg_volume baseline
  * ``handle_incoming_dataframe_daily`` -- daily OHLCV + ATR14 + Prev_Close/TR
                                        for the daily table and ATR seeding
  * ``build_last_atr_dict``          -- {symbol: latest ATR}   (feeds SessionStore)
  * ``build_last_prev_close_dict``   -- {symbol: latest close} (feeds SessionStore)
  * ``seed_and_enrich_intraday``     -- walk today's bars through
                                        SymbolSessionState.apply_bar; the
                                        primed rows on disk and the
                                        streaming rows that follow are
                                        guaranteed bit-identical.

Everything downstream of these transforms (SessionStore, apply_bar,
DB persist, live enrichment) uses lowercase column names.
"""
from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import time
from typing import Dict, List
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from data_sources._bar import IncomingBar
from indicators.atr  import atr_series
from indicators.rvol import avg_volume_model

from src.core.config             import settings
from src.helpers.handle_candles  import CandleRow
from indicators.session_state    import SymbolSessionState

logger = logging.getLogger(__name__)




def _intraday_frame(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    df = pd.DataFrame([asdict(bar) for bar in IncomingBar.from_raw_bars(bars)])
    df = df.drop(columns=[c for c in ("average", "barCount") if c in df.columns])

    dt = pd.to_datetime(df["date"], utc=True).dt.tz_convert(ZoneInfo(settings.TIMEZONE))
    df["date"]   = dt.dt.date
    df["time"]   = dt.dt.time
    df["symbol"] = symbol
    return df


# ---------------------------------------------------------------------------
# Focused pipeline transforms -- one function per output frame.
# Used by the IB three-way fetch path (see warmup_source.IBWarmupSource).
# ---------------------------------------------------------------------------


def bars_to_today_frame(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    df = _intraday_frame(bars, symbol)
    df = df[df["time"] >= time(11, 0)]
    return df[["symbol", "date", "time", "open", "high", "low", "close", "volume"]]


def bars_to_avg_volume_frame(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    df = _intraday_frame(bars, symbol)
    df = df[["symbol", "date", "time", "open", "high", "low", "close", "volume"]]
    return avg_volume_model(df)


def handle_incoming_dataframe_daily(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    df = pd.DataFrame([asdict(bar) for bar in IncomingBar.from_raw_bars(bars)])
    df["symbol"] = symbol

    df["prev_close"] = df["close"].shift(1)
    hl   = df["high"] - df["low"]
    h_pc = (df["high"] - df["prev_close"].fillna(df["high"])).abs()
    l_pc = (df["low"]  - df["prev_close"].fillna(df["low"])).abs()
    df["tr"]  = np.maximum.reduce([hl.values, h_pc.values, l_pc.values])
    df["atr"] = atr_series(df["high"], df["low"], df["close"])

    desired_order = [
        "symbol", "date", "open", "high", "low", "close", "volume",
        "average", "barCount", "prev_close", "tr", "atr",
    ]
    return df[[c for c in desired_order if c in df.columns]]


# ---------------------------------------------------------------------------
# Dict builders for SessionStore seeding
# ---------------------------------------------------------------------------


def build_last_atr_dict(daily_with_atr: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """Latest ATR per symbol from the daily-bars dict keyed by symbol."""
    return {sym: float(df["atr"].iloc[-1]) for sym, df in daily_with_atr.items()}


def build_last_prev_close_dict(daily_with_atr: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """
    Yesterday's close per symbol -- the last row of the daily frame.
    The daily fetch window ends at yesterday 23:59:59, so this is
    yesterday's session close (or Friday's on a Monday).
    """
    return {sym: float(df["close"].iloc[-1]) for sym, df in daily_with_atr.items()}


# ---------------------------------------------------------------------------
# Warmup enrichment via SessionStore.apply_bar
#
# The SAME apply_bar code produces the primed bars at boot AND enriches
# each incoming bar at runtime. Any drift between warmup and live is
# therefore impossible -- there is only one code path.
# ---------------------------------------------------------------------------


_ENRICHED_COLS = [
    "symbol", "date", "time", "open", "high", "low", "close",
    "volume", "vwap", "ema9", "avg_volume", "rvol", "relatr", "day_atr_ext",
]


def seed_and_enrich_intraday(
    store,                                     # SessionStore (fwd-ref to avoid cycle)
    today_intra:                dict[str, pd.DataFrame],
    past_intra:                 dict[str, pd.DataFrame],
    last_atr_per_symbol:        dict[str, float],
    last_prev_close_per_symbol: dict[str, float],
) -> dict[str, pd.DataFrame]:

    enriched: dict[str, pd.DataFrame] = {}

    for symbol, today_df in today_intra.items():
        session_date = today_df["date"].iloc[0] if not today_df.empty else None
        st: SymbolSessionState = store.init(symbol, session_date)
        st.atr        = last_atr_per_symbol.get(symbol)
        st.prev_close = last_prev_close_per_symbol.get(symbol)

        past_df = past_intra.get(symbol)
        st.rvol_baseline = (
            dict(zip(past_df["time"], past_df["avg_volume"]))
            if past_df is not None and not past_df.empty
            else {}
        )

        rows_out = []
        for _, r in today_df.iterrows():
            candle = CandleRow(
                symbol      = symbol,
                date        = r["date"],
                time        = r["time"],
                open        = float(r["open"]),
                high        = float(r["high"]),
                low         = float(r["low"]),
                close       = float(r["close"]),
                volume      = float(r["volume"]),
                vwap        = None,
                ema9        = None,
                avg_volume  = None,
                rvol        = None,
                relatr      = None,
                day_atr_ext = None,
            )
            st.apply_bar(candle)
            d = asdict(candle)
            rows_out.append({col: d[col] for col in _ENRICHED_COLS})

        enriched[symbol] = pd.DataFrame(rows_out, columns=_ENRICHED_COLS)
        logger.debug("%s -- last 10 primed rows:\n%s",
                     symbol, enriched[symbol].tail(10))

    return enriched
