"""
Strategy registry — a tiny dispatch layer so we can enable/disable strategies
from strategies.toml instead of commenting code in src/strategies.py.

Mental model
------------
Each strategy is registered once with:
  - name           : identifier used in strategies.toml
  - guard          : cheap predicate on the candle (e.g. `candle.relatR > 0`)
  - runner         : async fn taking (candle, History) and doing the work
  - needs_rows     : how many recent rows from the symbol's livestream table
                     the runner needs (0 = none). The dispatcher fetches the
                     *max* across active strategies ONCE per candle and
                     populates History.last_rows.
  - needs_session  : if True, fetch every row of today's session (Date == today
                     AND Time >= settings.SESSION_START) ONCE per candle and
                     populate History.session. Strategies that need
                     full intraday context (e.g. VWAP continuation, which has
                     to know what price did since the open) use this.

Loading toggles
---------------
`registry.load_toggles(path)` reads a TOML file shaped like:

    [strategies.<name>]
    enabled = true

and flips each spec's `enabled` flag accordingly. Names not present in the
TOML default to enabled=True. Names in the TOML that we never registered are
logged as warnings (catches typos).
"""
from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# A guard returns True if the strategy is applicable to this candle.
GuardFn = Callable[[Any], bool]


@dataclass
class History:
    """History context passed to every strategy runner.

    last_rows : the deepest ``last N`` rows requested by any active strategy.
                None when no active strategy asked for last-rows history.
    session   : every row of today's session (Date == candle.date AND
                Time >= settings.SESSION_START). None when no active strategy
                asked for session history.

    A runner picks whichever field it cares about — both can be populated
    simultaneously, and each kind of history is fetched at most once per
    candle.
    """
    last_rows: Optional[pd.DataFrame] = None
    session: Optional[pd.DataFrame] = None


# A runner always receives the same shape, so the dispatcher stays simple.
RunnerFn = Callable[[Any, History], Awaitable[None]]


@dataclass
class StrategySpec:
    name: str
    guard: GuardFn
    runner: RunnerFn
    needs_rows: int = 0
    needs_session: bool = False
    enabled: bool = True


class StrategyRegistry:
    def __init__(self) -> None:
        self._specs: list[StrategySpec] = []

    def register(
        self,
        name: str,
        *,
        guard: GuardFn,
        runner: RunnerFn,
        needs_rows: int = 0,
        needs_session: bool = False,
    ) -> None:
        if any(s.name == name for s in self._specs):
            raise ValueError(f"Strategy already registered: {name!r}")
        self._specs.append(
            StrategySpec(
                name=name,
                guard=guard,
                runner=runner,
                needs_rows=needs_rows,
                needs_session=needs_session,
            )
        )

    def load_toggles(self, path: Path) -> None:
        """Apply enabled/disabled flags from a TOML file. Missing file = all on."""
        if not path.exists():
            logger.warning(
                "Strategy toggles file not found: %s — leaving all strategies enabled.",
                path,
            )
            return

        with path.open("rb") as f:
            data = tomllib.load(f)
        toggles = data.get("strategies", {}) or {}

        for spec in self._specs:
            cfg = toggles.get(spec.name, {})
            spec.enabled = bool(cfg.get("enabled", True))

        unknown = set(toggles) - {s.name for s in self._specs}
        if unknown:
            logger.warning(
                "strategies.toml references unknown strategies: %s", sorted(unknown)
            )

        enabled = [s.name for s in self._specs if s.enabled]
        disabled = [s.name for s in self._specs if not s.enabled]
        logger.info(
            "Strategy registry loaded from %s — enabled=%s disabled=%s",
            path.name,
            enabled,
            disabled,
        )

    def active_for(self, candle: Any) -> list[StrategySpec]:
        """Specs that are both enabled AND whose guard fires for this candle."""
        return [s for s in self._specs if s.enabled and s.guard(candle)]

    def all(self) -> list[StrategySpec]:
        return list(self._specs)


# Module-level singleton — import this everywhere.
registry = StrategyRegistry()
