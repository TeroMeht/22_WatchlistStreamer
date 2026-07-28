"""
Shared visualization hook layer for entry strategies.

Each strategy has its own ``visualization/state.py`` module that owns
the per-symbol overlay data (candles, current reference, fires, etc.)
that the dashboard renders. This factory binds one hook set to that
module so a strategy body can call ``hooks.on_bar(...)`` without
importing viz directly -- swapping the backend (websocket, no-op for
tests, different store) becomes a one-line change at hook-creation
time.

Usage in a strategy:

    from src.strategies.hooks import make_hooks
    from .visualization import state as viz

    hooks = make_hooks(viz)

    # In the orchestrator body:
    hooks.on_bar(symbol, bar_time_local, bar)
    hooks.on_reference(symbol, breakout_level)
    hooks.on_breakout(symbol)
    hooks.on_fire(symbol, bar_time_local, close, ref_close, stop_level=stop_level)

The viz module must expose:

    record_5s_tick(symbol, bar_dt, open_, high, low, close)
    record_reference(symbol, ref_time, ref_close, ref_low, field=None)
    record_state(symbol, state)
    record_fire(symbol, bar_dt, close, stop_level, ref_close)
        stop_level may be None for alarm-only strategies.
    STATE_BREAKOUT: str  (constant)

All hooks are synchronous, side-effect-only, and return ``None``.
"""

from __future__ import annotations

from datetime import datetime
from types import ModuleType, SimpleNamespace
from typing import Optional

from ib_async import RealTimeBar


def make_hooks(viz: ModuleType) -> SimpleNamespace:
    """
    Build a hook set bound to ``viz`` (the strategy's visualization
    state module). Returns a SimpleNamespace with ``on_bar``,
    ``on_reference``, ``on_breakout``, ``on_fire``.
    """

    def on_bar(symbol: str, bar_time_local: datetime, bar: RealTimeBar) -> None:
        """Feed the current in-progress 2-min candle from one 5-sec tick."""
        viz.record_5s_tick(
            symbol,
            bar_time_local,
            float(bar.open_),
            float(bar.high),
            float(bar.low),
            float(bar.close),
        )

    def on_reference(symbol: str, breakout_level: object) -> None:
        viz.record_reference(
            symbol,
            breakout_level.ref_time,
            breakout_level.ref_close,
            breakout_level.ref_low,
            field=getattr(breakout_level, "ref_field", None),
        )

    def on_breakout(symbol: str) -> None:
        viz.record_state(symbol, viz.STATE_BREAKOUT)

    def on_fire(
        symbol: str,
        bar_time_local: datetime,
        bar_close: float,
        ref_close: float,
        stop_level: Optional[float] = None,
    ) -> None:
        """
        Record a completed fire. ``stop_level`` is optional for
        alarm-only strategies; the viz module stores ``None`` in that
        case and the dashboard skips the stop-line render for that fire.
        """
        viz.record_fire(symbol, bar_time_local, bar_close, stop_level, ref_close)

    return SimpleNamespace(
        on_bar=on_bar,
        on_reference=on_reference,
        on_breakout=on_breakout,
        on_fire=on_fire,
    )
