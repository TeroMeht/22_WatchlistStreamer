"""
Thin delegator over the shared ``indicators`` package.

Historical background: this module used to house 22's own copies of
VWAP / EMA / ATR / RelATR / DayAtrExt / RVOL. Those have moved to
``indicators`` (pip-installed editable from
``C:/codebase/prod/indicators``) and are the source of truth for both
22 and 32.

What still lives here:

  * ``calculate_14day_atr_df``  -- DataFrame-in / DataFrame-out
                                    wrapper around ``indicators.atr.atr_series``.
                                    Keeps the ``Prev_Close`` / ``TR`` /
                                    ``ATR`` columns the daily pipeline
                                    already relies on, and preserves
                                    the uppercase-column convention 22
                                    uses today (Phase C target).
  * ``calculate_avg_volume_model`` -- wrapper around
                                    ``indicators.rvol.avg_volume_model``,
                                    returning the same
                                    ``[Symbol, Time, Avg_volume]``
                                    frame the DB writers expect.
  * ``calculate_position_size``  -- unchanged; pure sizing helper, has
                                    no counterpart in indicators.

Everything else is gone -- the per-candle ``calculate_next_*``
functions moved to ``SymbolSessionState.apply_bar`` in
``src/streamer/session_state.py``, which calls ``indicators.next_*``
directly. If something still imports ``calculate_next_vwap`` /
``_ema9`` / ``_relatr`` / ``_day_atr_ext`` / ``_rvol``, that call
site needs to be routed through the session store instead.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from indicators.atr  import atr_series
from indicators.rvol import avg_volume_model


logger = logging.getLogger(__name__)


def calculate_14day_atr_df(data: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Add ATR14 to a daily OHLC DataFrame.

    Input columns: ``High``, ``Low``, ``Close`` (uppercase, one symbol
    per frame, sorted ascending by date).

    Output: input frame with ``Prev_Close``, ``TR``, and ``ATR``
    columns appended. Matches the historical shape 22's daily writer +
    ``build_last_atr_dict`` already consume.

    First-row TR falls back to ``High - Low`` (no previous close) --
    handled by ``indicators.atr.atr_series``.
    """
    df = data.copy()
    df['Prev_Close'] = df['Close'].shift(1)
    hl   = df['High'] - df['Low']
    h_pc = (df['High'] - df['Prev_Close'].fillna(df['High'])).abs()
    l_pc = (df['Low']  - df['Prev_Close'].fillna(df['Low'])).abs()
    df['TR'] = np.maximum.reduce([hl.values, h_pc.values, l_pc.values])
    df['ATR'] = atr_series(df['High'], df['Low'], df['Close'], span=period)
    return df


def calculate_avg_volume_model(day5_history_datas: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine N daily intraday frames and produce the winsorized per-slot
    baseline via ``indicators.rvol.avg_volume_model``.

    Winsorization at ``k=3.0`` is on by default (see
    ``indicators.rvol.DEFAULT_WINSOR_K``); pass through to the shared
    implementation keeps the exact 22 behavior.

    Input frames must carry columns: ``Symbol``, ``Date``, ``Time``,
    ``Volume`` (any extras are ignored). Output:
    ``[Symbol, Time, Avg_volume]``.
    """
    all_data = pd.concat(day5_history_datas, ignore_index=True)
    return avg_volume_model(all_data)


# Position size -- pure sizing helper, no indicator dependency.
def calculate_position_size(entry_price, stop_price, risk):
    try:
        risk_per_unit = entry_price - stop_price
        if risk_per_unit == 0:
            raise ValueError("Entry price and stop price cannot be the same.")
        position_size = abs(int(risk / risk_per_unit))
        return position_size
    except Exception as e:
        logging.error("Error calculating position size: %s", e)
        return None
