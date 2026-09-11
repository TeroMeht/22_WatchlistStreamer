"""
Unit tests for the shared fire pipeline (``src.strategies.actions.fire_signal``)
and its three call sites.

Refactor under test:
    * ``fire_signal`` was widened from an ORB-specific helper to the one
      function every strategy funnels its fire step through.
    * ORB, reversal, and vwap_continuation_long each call it now instead
      of hand-rolling ``generate_entry_order`` / ``generate_signal_alarm``
      / ``viz.record_fire`` / latch inline.
    * ``bar_time_local`` is derived inside ``fire_signal`` from
      ``candle.date`` + ``candle.time`` + ``settings.TIMEZONE`` -- no
      caller passes it.
    * ``stop_level`` is required (no more alarm-only path).

All I/O is mocked -- database, alarm sender, order writer, viz module.
Only the shape and ordering of the calls fire_signal makes are under test.

Run with:
    uv run pytest tests/test_fire_signal.py -v
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from indicators.candle_row import CandleRow

from src.core.config import settings
from src.strategies.actions import fire_signal


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _candle(
    *,
    symbol: str = "AAPL",
    d: date = date(2026, 9, 10),
    t: time = time(16, 32),
    close: float = 100.50,
) -> CandleRow:
    """Build a minimal CandleRow. Indicator fields are zeroed -- the fire
    step doesn't read them (they only matter for filter evaluation, which
    runs earlier in the strategy)."""
    return CandleRow(
        symbol=symbol,
        date=d,
        time=t,
        open=close, high=close, low=close, close=close,
        volume=1000.0,
        vwap=close, ema9=close, avg_volume=1000.0,
        rvol=1.0, relatr=1.0, day_atr_ext=0.0,
    )


@pytest.fixture
def viz() -> MagicMock:
    """Stand-in for a strategy's visualization state module. Only
    ``record_fire`` is exercised in the fire step -- the other viz
    surface methods (``record_filter_results``, ``record_reference``)
    belong to earlier phases."""
    m = MagicMock()
    m.record_fire = MagicMock()
    return m


@pytest.fixture
def patched_io():
    """Patch the two external side-effects fire_signal invokes so nothing
    hits the database or the alarm channel."""
    with patch(
        "src.strategies.actions.generate_entry_order", new_callable=AsyncMock,
    ) as order, patch(
        "src.strategies.actions.generate_signal_alarm", new_callable=AsyncMock,
    ) as alarm:
        yield order, alarm


# ---------------------------------------------------------------------------
# 1. Happy path: all four steps run, in order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_pipeline_runs_every_step_in_order(patched_io, viz):
    """Order matters:
        1. entry order
        2. alarm
        3. viz.record_fire
        4. mark_fired (last, so the latch only flips on success)
    """
    order, alarm = patched_io
    latch = MagicMock()

    parent = MagicMock()
    parent.attach_mock(order, "order")
    parent.attach_mock(alarm, "alarm")
    parent.attach_mock(viz.record_fire, "record_fire")
    parent.attach_mock(latch, "latch")

    await fire_signal(
        candle=_candle(),
        signal_name="orb_breakout",
        stop_level=98.75,
        viz=viz,
        ref_close=101.20,
        live_price=100.90,
        mark_fired=latch,
    )

    call_names = [c[0] for c in parent.mock_calls]
    assert call_names == ["order", "alarm", "record_fire", "latch"]


# ---------------------------------------------------------------------------
# 2. Downstream call shapes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entry_order_receives_candle_and_stop(patched_io, viz):
    order, _ = patched_io
    candle = _candle()

    await fire_signal(
        candle=candle, signal_name="s", stop_level=98.75, viz=viz,
    )

    order.assert_awaited_once_with(candle=candle, stop_level=98.75)


@pytest.mark.asyncio
async def test_alarm_receives_candle_and_signal_name(patched_io, viz):
    _, alarm = patched_io
    candle = _candle()

    await fire_signal(
        candle=candle, signal_name="reversal_long",
        stop_level=98.75, viz=viz,
    )

    alarm.assert_awaited_once_with(candle=candle, signal_name="reversal_long")


@pytest.mark.asyncio
async def test_record_fire_uses_explicit_ref_and_live_price(patched_io, viz):
    """ORB-style call: explicit ref_close (breakout level) and live_price
    (bar close, may differ from the CandleRow's synthesized close)."""
    candle = _candle(symbol="MSFT", close=100.50)

    await fire_signal(
        candle=candle, signal_name="orb_breakout",
        stop_level=98.75, viz=viz,
        ref_close=101.20, live_price=100.90,
    )

    viz.record_fire.assert_called_once()
    args, kwargs = viz.record_fire.call_args
    symbol, _bar_dt, live_price = args
    assert symbol == "MSFT"
    assert live_price == 100.90
    assert kwargs["stop_level"] == 98.75
    assert kwargs["ref_close"] == 101.20


@pytest.mark.asyncio
async def test_record_fire_defaults_ref_and_live_to_candle_close(patched_io, viz):
    """Candle-driven call (reversal / vwap): omit ref_close and
    live_price -- both should default to candle.close."""
    candle = _candle(close=100.50)

    await fire_signal(
        candle=candle, signal_name="reversal_long",
        stop_level=98.75, viz=viz,
    )

    args, kwargs = viz.record_fire.call_args
    assert args[2] == 100.50            # live_price positional
    assert kwargs["ref_close"] == 100.50


# ---------------------------------------------------------------------------
# 3. bar_time_local is derived from the candle -- no caller passes it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bar_time_local_derived_from_candle_and_settings_tz(patched_io, viz):
    candle = _candle(d=date(2026, 9, 10), t=time(16, 32))

    await fire_signal(
        candle=candle, signal_name="s", stop_level=98.75, viz=viz,
    )

    expected = datetime(2026, 9, 10, 16, 32, tzinfo=ZoneInfo(settings.TIMEZONE))
    args, _ = viz.record_fire.call_args
    assert args[1] == expected


@pytest.mark.asyncio
async def test_bar_time_local_is_tz_aware(patched_io, viz):
    """Regression guard: the record_fire consumer wants tz-aware -- a
    naive datetime here would silently break the dashboard alignment."""
    await fire_signal(
        candle=_candle(), signal_name="s", stop_level=98.75, viz=viz,
    )
    args, _ = viz.record_fire.call_args
    bar_dt: datetime = args[1]
    assert bar_dt.tzinfo is not None


# ---------------------------------------------------------------------------
# 4. stop_level: always coerced to float, always forwarded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_level_coerced_to_float_downstream(patched_io, viz):
    """asyncpg / the dashboard both want a float, not a Decimal.
    ``fire_signal`` normalises so callers don't each have to."""
    order, _ = patched_io

    await fire_signal(
        candle=_candle(), signal_name="s",
        stop_level=Decimal("98.75"), viz=viz,
    )

    order.assert_awaited_once()
    order_kwargs = order.await_args.kwargs
    assert order_kwargs["stop_level"] == 98.75
    assert isinstance(order_kwargs["stop_level"], float)

    _, viz_kwargs = viz.record_fire.call_args
    assert isinstance(viz_kwargs["stop_level"], float)


# ---------------------------------------------------------------------------
# 5. mark_fired: called only when passed, last
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_fired_called_with_symbol_when_provided(patched_io, viz):
    latch = MagicMock()
    await fire_signal(
        candle=_candle(symbol="TSLA"), signal_name="s",
        stop_level=1.0, viz=viz, mark_fired=latch,
    )
    latch.assert_called_once_with("TSLA")


@pytest.mark.asyncio
async def test_mark_fired_omitted_is_safe(patched_io, viz):
    """Reversal path: no latch. Must not blow up, must still record fire."""
    await fire_signal(
        candle=_candle(), signal_name="reversal_long",
        stop_level=98.75, viz=viz,
    )
    viz.record_fire.assert_called_once()


# ---------------------------------------------------------------------------
# 6. Call-site smoke tests -- each strategy hands fire_signal the right shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reversal_strategy_call_shape():
    """ReversalStrategy.run must call fire_signal WITHOUT mark_fired
    (reversal fires repeatedly per session by design)."""
    from src.strategies.reversal_shared.strategy_class import ReversalStrategy

    filters = MagicMock()
    filters.evaluate_filters = AsyncMock(return_value=(True, {"gate": True}))
    filters.format_summary = MagicMock(return_value="ok")

    viz_mod = MagicMock()
    strat = ReversalStrategy(
        direction="long", signal_name="reversal_long",
        viz=viz_mod, filters=filters,
    )

    candle = _candle()
    with patch(
        "src.strategies.reversal_shared.strategy_class.get_last_rows",
        new_callable=AsyncMock, return_value=MagicMock(),
    ), patch(
        "src.strategies.reversal_shared.strategy_class.detect_stoplevel",
        return_value=98.75,
    ), patch(
        "src.strategies.reversal_shared.strategy_class.fire_signal",
        new_callable=AsyncMock,
    ) as fire:
        await strat.run(candle)

    fire.assert_awaited_once()
    kwargs = fire.await_args.kwargs
    assert kwargs["candle"] is candle
    assert kwargs["signal_name"] == "reversal_long"
    assert kwargs["stop_level"] == 98.75
    assert kwargs["viz"] is viz_mod
    assert "mark_fired" not in kwargs


@pytest.mark.asyncio
async def test_vwap_continuation_long_call_shape():
    """vwap_continuation_long must pass mark_fired -- it's one-shot per
    session per symbol."""
    from src.strategies.vwap_continuation_long import strategy as mod

    candle = _candle()
    with patch.object(mod, "evaluate_filters", new_callable=AsyncMock,
                      return_value=(True, {})), \
         patch.object(mod, "format_summary", return_value="ok"), \
         patch.object(mod, "has_fired", return_value=False), \
         patch.object(mod, "get_last_rows", new_callable=AsyncMock,
                      return_value=MagicMock()), \
         patch.object(mod, "detect_stoplevel", return_value=98.75), \
         patch.object(mod, "fire_signal", new_callable=AsyncMock) as fire, \
         patch.object(mod, "viz"):
        await mod.vwap_continuation_long(candle)

    fire.assert_awaited_once()
    kwargs = fire.await_args.kwargs
    assert kwargs["candle"] is candle
    assert kwargs["signal_name"] == "vwap_continuation_long"
    assert kwargs["stop_level"] == 98.75
    assert kwargs["mark_fired"] is mod.mark_fired


@pytest.mark.asyncio
async def test_vwap_continuation_long_skips_fire_when_latched():
    """Sanity: the one-shot latch is honored -- if has_fired returns
    True, fire_signal is NOT called."""
    from src.strategies.vwap_continuation_long import strategy as mod

    with patch.object(mod, "evaluate_filters", new_callable=AsyncMock,
                      return_value=(True, {})), \
         patch.object(mod, "format_summary", return_value="ok"), \
         patch.object(mod, "has_fired", return_value=True), \
         patch.object(mod, "fire_signal", new_callable=AsyncMock) as fire, \
         patch.object(mod, "viz"):
        await mod.vwap_continuation_long(_candle())

    fire.assert_not_awaited()
