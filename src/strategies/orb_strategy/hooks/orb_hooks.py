"""
Visualization hooks for the ORB long strategy.

Wraps every ``orb_strategy.visualization.orb_state.record_*`` call the strategy
needs so the strategy body doesn't import ``viz`` directly. This is the
seam that lets us swap the visualization backend later (websocket,
different store, no-op for tests) without touching strategy logic.

All functions are synchronous, side-effect-only, and return ``None``.
"""

from __future__ import annotations

from datetime import datetime

from ib_async import RealTimeBar

from ..visualization import orb_state as viz


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


def on_reference(symbol: str, ref: object) -> None:
    viz.record_reference(
        symbol,
        ref.ref_time,
        ref.ref_close,
        ref.ref_low,
        field=getattr(ref, "ref_field", None),
    )


def on_breakout(symbol: str) -> None:
    viz.record_state(symbol, viz.STATE_BREAKOUT)


def on_fire(
    symbol: str,
    bar_time_local: datetime,
    bar_close: float,
    stop_level: float,
    ref_close: float,
) -> None:
    """Record a completed fire on the visualization."""
    viz.record_fire(symbol, bar_time_local, bar_close, stop_level, ref_close)
