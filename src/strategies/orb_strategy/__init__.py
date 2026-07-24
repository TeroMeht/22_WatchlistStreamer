"""
ORB long strategy package.

Public API (all other imports from within the package are considered
internal and may change without notice):

    orb_breakout_long(bar, symbol)
        Called by the realtime dispatcher on every 5-sec bar.

    ORB_TEST_MODE_USE_LAST_CANDLE
        Flip to switch reference between the 16:32 candle (production)
        and the last-two-candles range (test mode).
"""

from .config import ORB_TEST_MODE_USE_LAST_CANDLE
from .strategy import orb_breakout_long

__all__ = [
    "orb_breakout_long",
    "ORB_TEST_MODE_USE_LAST_CANDLE",
]
