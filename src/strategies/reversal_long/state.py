"""
Session-scoped module state for the reversal_long breakout strategy.

Currently holds only the one-shot fire latch per symbol. Once the
strategy fires for a symbol, it's latched for the rest of the session --
no re-fires until the streamer restarts. Process-scoped in memory;
restart wipes the cache and re-arms every symbol.
"""

from __future__ import annotations


# --- One-shot fire latch -----------------------------------------------------
_fired_this_session: dict[str, bool] = {}


def mark_fired(symbol: str) -> None:
    """Latch ``symbol`` so this strategy will not fire again until restart."""
    _fired_this_session[symbol.upper()] = True


def has_fired(symbol: str) -> bool:
    """True if the strategy has already fired for ``symbol`` this session."""
    return _fired_this_session.get(symbol.upper(), False)
