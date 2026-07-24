"""
ORB long signal detection.

Edge-triggered breakout detector. The strategy fires only when price
crosses UP through the reference close -- staying above the reference
on subsequent bars does not re-fire. Symmetric to standard
level-breakout logic, kept isolated so the strategy body doesn't have
to reason about state.

The per-symbol "was above ref on the previous bar?" memory lives here,
private to the signals module. Reset happens naturally on streamer
restart.
"""

from __future__ import annotations

from typing import NamedTuple


# Distinct outcomes the edge detector can report. Kept as plain strings
# so log lines and future serialization stay flat and human-readable.
EDGE_BELOW: str = "below"                   # price at or below ref
EDGE_CONTINUATION: str = "continuation"     # above ref but was also above last bar
EDGE_FRESH_CROSSING: str = "fresh_crossing" # first bar to cross above ref


class EdgeEvent(NamedTuple):
    kind: str          # one of EDGE_*
    is_above: bool     # bar close > ref close
    was_above: bool    # previous bar was above ref


# Private state: {SYMBOL_UPPER: was_above_last_bar}. Only ``detect_edge``
# touches it. Cleared implicitly by streamer restart.
_last_above_ref: dict[str, bool] = {}


def detect_breakout(symbol: str, bar_close: float, ref_close: float) -> EdgeEvent:
    """
    Classify the current bar relative to ref_close, using the previous
    bar's state to distinguish fresh crossings from continuations.
    Always updates the internal memory as a side effect (this is the
    detector's whole job -- caller doesn't need to remember).
    """
    key = symbol.upper()
    was_above = _last_above_ref.get(key, False)
    is_above = bar_close > ref_close
    _last_above_ref[key] = is_above

    if not is_above:
        return EdgeEvent(EDGE_BELOW, False, was_above)
    if was_above:
        return EdgeEvent(EDGE_CONTINUATION, True, True)
    return EdgeEvent(EDGE_FRESH_CROSSING, True, False)


def describe_status(event: EdgeEvent) -> str:
    """One-line status for the per-bar check log."""
    if event.kind == EDGE_BELOW:
        return "no breakout yet"
    if event.kind == EDGE_CONTINUATION:
        return "above ref (continuation, no new fire)"
    return "BREAKOUT (crossing above ref)"
