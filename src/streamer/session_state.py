"""
Per-symbol running state + the O(1) ``apply_bar`` that folds one new
CandleRow into it via the shared ``indicators`` package.

Mirrors 32_smsystem's ``SymbolSessionState``; kept in sync so both
projects can eventually share the same class outright once column
names + candle types are also unified.

Field roles (22-specific naming preserved for this phase):

    atr         -- yesterday's ATR14 (from daily bars via
                   ``build_last_atr_dict``). Divisor for RelATR +
                   DayAtrExt. ``None`` if the symbol had no daily
                   fetch; both indicators are then skipped and left
                   as ``None`` on the candle.
    prev_close  -- yesterday's daily close (from
                   ``build_last_prev_close_dict``). Feeds DayAtrExt.
                   ``None`` -> DayAtrExt skipped.
    rvol_baseline / baseline_history_sum
                -- rvol_baseline is the seeded per-slot avg volume
                   for this symbol (dict[time, float]);
                   baseline_history_sum is the running Sigma slot_avg
                   seen this session.
    cum_pv / cum_vol
                -- running (Sigma OHLC4*volume, Sigma volume) pair for
                   the O(1) session VWAP. cum_vol also serves as the
                   running vol sum for RVOL cum (see note in
                   apply_bar on ordering).
    prev_ema    -- previous EMA9 value, or ``None`` before the first
                   bar (matches ``ewm(adjust=False)`` seeding).

Note the 22-specific candle-field names retained on ``CandleRow``:
``relatr`` (capital R) and ``rvol`` (not ``rvol_cum``). These are
Phase-C harmonization targets; kept as-is here so Phase A+B stays
mechanical.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Optional

from indicators.day_atr_ext import next_day_atr_ext
from indicators.ema         import next_ema
from indicators.relatr      import next_relatr
from indicators.rvol        import next_rvol_cum
from indicators.vwap        import next_vwap

from src.helpers.handle_candles import CandleRow


EMA_SPAN: int = 9


@dataclass
class SymbolSessionState:
    symbol: str
    session_date: date  # Helsinki session date (matches CandleRow.date)
    # Seeded once at warmup -- read-only afterward.
    atr:           Optional[float] = None
    prev_close:    Optional[float] = None
    rvol_baseline: dict[time, float] = field(default_factory=dict)
    # Running sums advanced by apply_bar every call.
    cum_pv:               float = 0.0    # Sigma OHLC4*volume  (VWAP numerator)
    cum_vol:              float = 0.0    # Sigma volume        (VWAP denom + RVOL num)
    baseline_history_sum: float = 0.0    # Sigma slot_avg      (RVOL denom)
    prev_ema:             Optional[float] = None

    def apply_bar(self, candle: CandleRow) -> CandleRow:
        """
        Enrich ``candle`` in place using this state, advance the
        running fields, and return the same CandleRow for chaining.

        Ordering matters: RVOL cum runs BEFORE VWAP because both fold
        ``candle.volume`` into ``self.cum_vol``, and RVOL uses the
        pre-fold value as its ``running_vol_sum``. VWAP commits the
        post-fold value on the same call.
        """
        slot_avg = self.rvol_baseline.get(candle.time, 0.0)
        candle.avg_volume = float(slot_avg)

        # RVOL first (reads pre-fold cum_vol).
        candle.rvol, _, self.baseline_history_sum = next_rvol_cum(
            candle.volume,
            running_vol_sum      = self.cum_vol,
            slot_avg             = slot_avg,
            running_baseline_sum = self.baseline_history_sum,
        )

        # VWAP (folds candle.volume into cum_vol; commit post-fold).
        candle.vwap, self.cum_pv, self.cum_vol = next_vwap(
            candle.open, candle.high, candle.low, candle.close, candle.volume,
            cum_pv=self.cum_pv, cum_vol=self.cum_vol,
        )

        # EMA9 (streaming; None on first bar returns the close).
        candle.ema9 = self.prev_ema = next_ema(
            candle.close, prev_ema=self.prev_ema, span=EMA_SPAN,
        )

        # RelATR + DayAtrExt -- skip when their inputs are missing so
        # the DB columns stay nullable instead of throwing. Common
        # cause: daily backfill missed the symbol.
        candle.relatr = (
            next_relatr(candle.vwap, candle.close, self.atr)
            if self.atr else None
        )
        candle.day_atr_ext = (
            next_day_atr_ext(self.prev_close, candle.close, self.atr)
            if self.atr and self.prev_close is not None else None
        )

        return candle


class SessionStore:
    """symbol -> SymbolSessionState. New instance per session boundary."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, SymbolSessionState] = {}

    def init(self, symbol: str, session_date: date) -> SymbolSessionState:
        st = SymbolSessionState(symbol=symbol, session_date=session_date)
        self._by_symbol[symbol] = st
        return st

    def get(self, symbol: str) -> SymbolSessionState:
        st = self._by_symbol.get(symbol)
        if st is None:
            raise KeyError(
                f"SessionStore has no state for {symbol!r} -- seed missed it?"
            )
        return st

    def __contains__(self, symbol: str) -> bool:
        return symbol in self._by_symbol
