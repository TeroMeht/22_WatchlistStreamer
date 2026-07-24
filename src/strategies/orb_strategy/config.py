"""
ORB long strategy configuration.

Plain constants only -- no state, no I/O. If any of these want to become
user-configurable at runtime, promote them to ``src.core.config.settings``
and read from there instead of importing from this module.
"""

from __future__ import annotations


# Stop level is anchored $0.02 below the reference candle's Low. Absolute
# dollars, not a percentage.
ORB_STOP_OFFSET: float = 0.02

# Minimum Rvol on the most recent 2-min candle for the Rvol filter to pass.
# Only consulted if the filter is enabled in strategy.py.
ORB_MIN_RVOL: float = 3.0

# --- TEST MODE ---------------------------------------------------------------
# When True, the reference candle is the MOST RECENT completed 2-min candle
# for the symbol (whichever it happens to be) instead of the 16:32 candle
# from ``settings.TIMEZONE``. This makes the breakout logic testable at any
# time of day. Set back to False before real trading.
ORB_TEST_MODE_USE_LAST_CANDLE: bool = True
