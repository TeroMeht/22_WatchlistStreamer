"""
CSV-driven replay of 5-sec bars.

Mirrors the live path (``monitor_tickers`` -> ``process_bar``) but feeds
bars from disk instead of subscribing to IB.

CSV format (with header row):

    time,symbol,open,high,low,close,volume,wap,count

* ``time``   -- naive datetime in ``settings.TIMEZONE`` (Helsinki in
                prod), e.g. ``2026-07-30 16:30:00`` or ISO ``T`` form.
                Date is embedded per row, so replays can span multiple
                sessions in one file.
* ``symbol`` -- ticker for that row. Read from the CSV rather than the
                filename so a mislabeled file can't cause silent
                cross-contamination between symbols.

Discovery: every ``*.csv`` under ``settings.REPLAY_DATA_DIR`` (flat, no
date subfolder). For each file we peek at the first data row to learn
its symbol, drop the file if the symbol isn't in ``valid_tickers``
(watchlist filter), then replay it.

Contract with ``process_bar``: ``process_bar`` does
``bar.date.replace(tzinfo=UTC).astimezone(settings.TIMEZONE)``. So
whatever we put in ``bar.date`` must be a **naive UTC** datetime --
same shape the IB path yields inside ``IncomingBar`` after conversion
from ``ib_async.RealTimeBar.time``. The Helsinki-local timestamp from
the CSV is therefore localized -> UTC -> stripped of tzinfo before
being handed to ``process_bar``.

Timing: rows for a single symbol are dispatched sequentially with
``asyncio.sleep((t2 - t1) / REPLAY_SPEED)`` between them; multiple
symbols run concurrently via ``asyncio.gather`` so cross-symbol
timestamps stay aligned. ``REPLAY_SPEED = 0`` skips all sleeps (as fast
as the event loop can drain).
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from data_sources._bar import IncomingBar

from src.core.config import settings
from src.helpers.process_incoming_data import process_bar

logger = logging.getLogger(__name__)


# Cached across the process lifetime -- the CSV set doesn't change
# mid-run and we don't want to re-open files on every warmup call.
_replay_start_cache: datetime | None = None


def get_replay_start_datetime() -> datetime:
    """
    Earliest bar timestamp across all CSVs in ``REPLAY_DATA_DIR`` that
    passes the ``REPLAY_START_TIME`` cutoff (if any), returned as a
    tz-aware datetime in ``settings.TIMEZONE``.

    """
    global _replay_start_cache
    if _replay_start_cache is not None:
        return _replay_start_cache

    data_path = Path(settings.REPLAY_DATA_DIR)
    if not data_path.exists():
        raise RuntimeError(f"REPLAY_DATA_DIR does not exist: {data_path}")

    csv_files = (
        [data_path] if data_path.is_file()
        else sorted(data_path.glob("*.csv"))
    )

    tz = ZoneInfo(settings.TIMEZONE)
    cutoff = settings.REPLAY_START_TIME  # datetime.time or None
    candidates: list[datetime] = []

    for path in csv_files:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                ts = _parse_ts(row.get("time", ""))
                if ts is None:
                    continue
                # Normalize to tz-aware in settings.TIMEZONE.
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=tz)
                else:
                    ts = ts.astimezone(tz)
                # Enforce the optional REPLAY_START_TIME cutoff BEFORE
                # accepting this row as the file's start. Otherwise the
                # anchor would fall in the skipped pre-cutoff region
                # and warmup would end at the wrong instant.
                if cutoff is not None and ts.timetz().replace(tzinfo=None) < cutoff:
                    continue
                candidates.append(ts)
                break  # first row past the cutoff per file is enough

    if not candidates:
        raise RuntimeError(
            f"Cannot determine replay start: no parseable timestamps in "
            f"{data_path} at or after REPLAY_START_TIME={cutoff}"
        )

    _replay_start_cache = min(candidates)
    logger.info(
        "[replay] start datetime resolved to %s (cutoff=%s)",
        _replay_start_cache, cutoff,
    )
    return _replay_start_cache


def get_effective_today() -> date:
    """
    Return the date the streamer should treat as "today":
      * replay mode -> the replay start's date (from the CSVs)
      * live mode   -> wall-clock ``date.today()``

    Used anywhere a pipeline step splits historical data into
    today-vs-past (e.g. the Rvol pre-session split in
    ``handle_incoming_dataframe_intradays_volume``).
    """
    if settings.MODE == "replay":
        return get_replay_start_datetime().date()
    return date.today()


# Fallback strptime formats for the space-separator ``bars_5s.log``
# shape. ISO forms (with or without tz offset) go through
# ``datetime.fromisoformat`` first.
_TS_FALLBACK_FORMATS = ("%Y-%m-%d %H:%M:%S",)


def _parse_ts(raw: str) -> datetime | None:
    """
    Parse a CSV timestamp string. Handles:
      * ISO with tz offset  -- ``2026-07-30T16:34:07+03:00``
      * ISO naive           -- ``2026-07-30T16:34:07``
      * Space-separated     -- ``2026-07-30 16:34:07`` (bars_5s.log)

    Returns ``None`` if nothing parses -- caller treats that as "not a
    data row" (typically the header, though DictReader normally strips it).
    Returned datetime may be naive OR tz-aware; caller normalizes to
    naive UTC.
    """
    raw = raw.strip()
    # fromisoformat handles both naive and tz-aware ISO strings in 3.11+;
    # in 3.10 it accepts ``+HH:MM`` offsets too. Fast path.
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in _TS_FALLBACK_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _row_to_bar(row: dict, tz: ZoneInfo) -> tuple[str, IncomingBar] | None:
    """
    Convert one CSV row (``dict`` from ``DictReader``) into a
    ``(symbol, bar)`` pair. Returns ``None`` for rows we can't parse --
    logged at warning level so bad data is visible without killing the run.

    Emits a canonical ``IncomingBar`` -- same shape the IB path
    produces inside ``IBRealtimeSource``. CSV field mapping:
    ``time``   -> ``IncomingBar.date`` (naive UTC datetime),
    ``open``   -> ``.open``,   ``high`` -> ``.high``,
    ``low``    -> ``.low``,    ``close`` -> ``.close``,
    ``volume`` -> ``.volume``,
    ``wap``    -> ``.average`` (IB's WAP == IncomingBar.average),
    ``count``  -> ``.barCount``.
    """
    parsed_ts = _parse_ts(row.get("time", ""))
    if parsed_ts is None:
        logger.warning("Skipping row with unparseable time: %r", row.get("time"))
        return None

    symbol = (row.get("symbol") or "").strip()
    if not symbol:
        logger.warning("Skipping row with empty symbol: %r", row)
        return None

    # Normalize to naive UTC so bar.date matches the shape the IB path
    # yields (RealTimeBar.time is naive UTC; IncomingBar.from_realtime
    # carries that through). process_bar does bar.date.replace(tzinfo=UTC).
    # If the CSV timestamp already carries a tz offset (e.g. +03:00) we
    # trust it; otherwise assume it's in settings.TIMEZONE (Helsinki).
    if parsed_ts.tzinfo is None:
        parsed_ts = parsed_ts.replace(tzinfo=tz)
    ts_utc_naive = parsed_ts.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    try:
        bar = IncomingBar(
            date     = ts_utc_naive,
            open     = float(row["open"]),
            high     = float(row["high"]),
            low      = float(row["low"]),
            close    = float(row["close"]),
            volume   = float(row["volume"]),
            average  = float(row["wap"]),
            barCount = int(float(row.get("count") or 0)),
        )
    except (KeyError, ValueError) as e:
        logger.warning("Skipping malformed row %r: %s", row, e)
        return None

    return symbol, bar


def _load_csv(path: Path, tz: ZoneInfo) -> tuple[str | None, list[IncomingBar]]:
    """
    Read one CSV into ``(symbol, bars)``.

    Symbol is taken from the first successfully parsed row; if subsequent
    rows disagree they're dropped with a warning (one file = one symbol).
    Bars are returned in file order, which for a well-formed export is
    already chronological.

    Applies ``settings.REPLAY_START_TIME`` (if set) as a per-row
    wall-clock cutoff -- rows whose local time-of-day is before the
    cutoff are silently dropped. Matches the anchor logic in
    ``get_replay_start_datetime`` so both dispatch AND warmup start at
    the same instant.
    """
    bars: list[IncomingBar] = []
    file_symbol: str | None = None
    cutoff = settings.REPLAY_START_TIME  # datetime.time or None
    skipped_before_cutoff = 0

    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            parsed = _row_to_bar(row, tz)
            if parsed is None:
                continue
            sym, bar = parsed
            if file_symbol is None:
                file_symbol = sym
            elif sym != file_symbol:
                logger.warning(
                    "%s: row symbol %r != file symbol %r -- skipping",
                    path.name, sym, file_symbol,
                )
                continue

            # Wall-clock cutoff. ``bar.date`` is naive UTC (matches
            # the IB path's IncomingBar shape -- see the module
            # docstring), so convert back to the display tz to compare
            # against a local time-of-day like 16:30.
            if cutoff is not None:
                local_t = (
                    bar.date.replace(tzinfo=ZoneInfo("UTC"))
                    .astimezone(tz)
                    .time()
                )
                if local_t < cutoff:
                    skipped_before_cutoff += 1
                    continue

            bars.append(bar)

    if skipped_before_cutoff:
        logger.info(
            "[replay] %s: dropped %d rows before REPLAY_START_TIME=%s",
            path.name, skipped_before_cutoff, cutoff,
        )

    return file_symbol, bars


def _discover_replays(valid_tickers: set[str]) -> dict[str, list[IncomingBar]]:
    """
    Scan ``REPLAY_DATA_DIR`` for ``*.csv`` files and return
    ``{symbol: bars}`` for every file whose symbol is in ``valid_tickers``.
    Files for other symbols are skipped with a log line so it's obvious
    what got picked up.
    """
    data_dir = Path(settings.REPLAY_DATA_DIR)
    if not data_dir.exists():
        raise RuntimeError(
            f"REPLAY_DATA_DIR does not exist: {data_dir}"
        )

    tz = ZoneInfo(settings.TIMEZONE)
    replays: dict[str, list[IncomingBar]] = {}

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("[replay] no CSVs found in %s", data_dir)
        return replays

    for path in csv_files:
        symbol, bars = _load_csv(path, tz)
        if symbol is None:
            logger.warning("[replay] %s: no valid rows -- skipping", path.name)
            continue
        if symbol not in valid_tickers:
            logger.info(
                "[replay] %s: symbol %s not in watchlist -- skipping",
                path.name, symbol,
            )
            continue
        if symbol in replays:
            logger.warning(
                "[replay] duplicate CSV for %s (%s) -- keeping the first",
                symbol, path.name,
            )
            continue
        replays[symbol] = bars
        logger.info("[replay] loaded %d bars for %s from %s",
                    len(bars), symbol, path.name)

    return replays


async def _replay_symbol(
    candle_store, atr, prev_close, symbol: str, bars: list[IncomingBar],
) -> None:
    """
    Drive one symbol's replay: for each bar, await ``process_bar`` (same
    call the live ``on_bar`` handler makes), then sleep the wall-clock
    gap to the next bar divided by ``REPLAY_SPEED``.

    Errors in ``process_bar`` are logged and the loop continues -- one
    bad bar shouldn't abort the whole replay.
    """
    if not bars:
        return

    speed = settings.REPLAY_SPEED
    prev_ts = None

    for bar in bars:
        if prev_ts is not None and speed > 0:
            gap = (bar.date - prev_ts).total_seconds() / speed
            if gap > 0:
                await asyncio.sleep(gap)
        prev_ts = bar.date

        try:
            await process_bar(candle_store, atr, prev_close, symbol, bar)
        except Exception:
            logger.exception(
                "[replay] process_bar failed for %s at %s", symbol, bar.date,
            )

    logger.info("[replay] finished %s (%d bars)", symbol, len(bars))


async def run_replay(candle_store, last_atr_dict: dict, last_prev_close_dict: dict, valid_tickers: list) -> None:
    """
    Replay-mode counterpart to ``run_streamer``. Discovers CSVs in
    ``REPLAY_DATA_DIR``, filters to the watchlist, then fans out one
    coroutine per symbol so cross-symbol timing stays aligned. Returns
    when every symbol's replay finishes (unlike live mode, which runs
    forever).
    """
    replays = _discover_replays(set(valid_tickers))
    if not replays:
        logger.error(
            "[replay] no CSVs matched the watchlist in %s -- nothing to do",
            settings.REPLAY_DATA_DIR,
        )
        return

    logger.info(
        "\n\nStarting replay: speed= %s symbols= %s",
        settings.REPLAY_SPEED, sorted(replays),
    )
    await asyncio.gather(*[
        _replay_symbol(
            candle_store,
            last_atr_dict.get(sym),
            last_prev_close_dict.get(sym),
            sym,
            bars,
        )
        for sym, bars in replays.items()
    ])
    logger.info("Replay complete.")
