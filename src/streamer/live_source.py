"""
Project-local live-source abstraction.

Two runtime modes today:

    * IB real-time bars    (``settings.MODE == "live"``)
    * CSV replay           (``settings.MODE == "replay"``)

Both drive the same downstream sink: ``process_bar`` per 5-sec bar,
per symbol. ``LiveSource`` hides the fan-out and the mode choice from
``data_pipe`` -- the branch on ``settings.MODE`` lives in
``make_live_source``.

IB path now composes ``IBRealtimeSource`` from
``data_sources.ib._source``, which internally fans
``reqRealTimeBars`` per symbol and converts each ``RealTimeBar`` to
``IncomingBar`` before invoking the callback. Replay path emits
``IncomingBar`` directly (see ``replay._row_to_bar``). ``process_bar``
therefore only ever sees ``IncomingBar``; ``ib_async`` is not on its
type surface.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from data_sources._bar       import IncomingBar
from data_sources.ib._client import IBSource
from data_sources.ib._source import IBRealtimeSource

from src.core.config                    import settings
from src.helpers.process_incoming_data  import CandleStore, process_bar
from src.streamer                       import replay


logger = logging.getLogger(__name__)


# =============================================================================
# Contract
# =============================================================================


class LiveSource(ABC):
    """
    Project-local abstraction over 'drive per-symbol 5-sec bars into
    ``process_bar``'. IB live blocks forever; replay returns when the
    CSV set is exhausted -- ``run`` matches both.
    """

    @abstractmethod
    async def run(
        self, valid_tickers: list, last_atr_dict: dict,
    ) -> None:
        """Feed bars until cancelled (live) or exhausted (replay)."""


# =============================================================================
# IB implementation -- real-time subscription across symbols
# =============================================================================


class IBLiveSource(LiveSource):
    """
    Wraps ``IBRealtimeSource`` and threads per-symbol ATR into the
    ``process_bar`` callback. The ABC's multi-symbol ``subscribe``
    handles the fan-out and per-symbol Ticker plumbing.
    """

    def __init__(self, source: IBSource):
        self._realtime = IBRealtimeSource(source)

    async def run(self, valid_tickers, last_atr_dict):
        logger.info("Starting live monitoring...")
        candle_store = CandleStore()

        async def on_bar(bar: IncomingBar, symbol: str) -> None:
            atr = last_atr_dict.get(symbol)
            await process_bar(candle_store, atr, symbol, bar)

        await self._realtime.subscribe(list(valid_tickers), on_bar)


# =============================================================================
# Replay implementation -- CSV-driven playback
# =============================================================================


class ReplayLiveSource(LiveSource):

    async def run(self, valid_tickers, last_atr_dict):
        logger.info(
            "Starting replay mode (speed=%s, data_dir=%s)...",
            settings.REPLAY_SPEED, settings.REPLAY_DATA_DIR,
        )
        candle_store = CandleStore()
        await replay.run_replay(candle_store, last_atr_dict, valid_tickers)


# =============================================================================
# Factory
# =============================================================================


def make_live_source(ib_source: Optional[IBSource]) -> LiveSource:
    """
    Pick the live-source implementation based on ``settings.MODE``.

    ``ib_source`` can be ``None`` when ``MODE=replay``. Raises if
    ``MODE=live`` but ``ib_source`` is ``None`` -- means the caller
    skipped IB in ``initialize_app`` even though live mode requires it.
    """
    if settings.MODE == "replay":
        logger.info("LiveSource: Replay (CSV)")
        return ReplayLiveSource()
    if settings.MODE == "live":
        if ib_source is None:
            raise RuntimeError(
                "MODE=live requires an IBSource; initialize_app returned None."
            )
        logger.info("LiveSource: IB real-time")
        return IBLiveSource(ib_source)
    raise ValueError(f"unknown MODE: {settings.MODE!r}")
