"""
Fetch historical 5-second bars from Interactive Brokers and write them
to a CSV in the EXACT format bars_5s.log uses in prod
(src/helpers/process_incoming_data.py), so the file can be fed straight
back into the replay path with no reshaping.

Line format (no header):
    YYYY-MM-DD HH:MM:SS,SYMBOL,open,high,low,close,volume,wap,count

Prices/volume/wap use %.4f, count is int, times are in TIMEZONE (local).

Edit the CONFIG section below, then run:
    python ib_tickdata.py

Requires:
    pip install ib_async
    TWS or IB Gateway running with API enabled.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from ib_async import IB, Stock

# ============ CONFIG ============
SYMBOL      = "CRWD"
DATE        = "2026-08-28"          # YYYY-MM-DD (local calendar date)
EXCHANGE    = "SMART"
CURRENCY    = "USD"

HOST        = "127.0.0.1"
PORT        = 4002                  # 7497 paper TWS, 7496 live TWS, 4002/4001 gateway
CLIENT_ID   = 20

USE_RTH     = False                 # True = regular trading hours only
TIMEZONE    = "Europe/Helsinki"     # output timestamp tz (matches settings.TIMEZONE)

# Strategy row that gets attached to SYMBOL when the watchlist SQL is
# regenerated at the end of the run. Keep in the CONFIG block so a single
# edit to (SYMBOL, DATE, STRATEGY) drives the CSV name, the CSV contents,
# and the watchlist row the replay run will actually pick up.
STRATEGY    = "vwap_continuation_long"

# Hard-coded output directory + file. Every replay run points the streamer
# at OUT_DIR, so we scrub *.csv there before fetching -- otherwise stale
# files from previous symbols/dates would silently get replayed alongside
# the new one.
OUT_DIR = Path(r"C:\codebase\prod\22_WatchlistStreamer\replay")
OUT_CSV = OUT_DIR / f"{SYMBOL}_{DATE}_5s.csv"

# Watchlist bootstrap SQL emitted at the end of the run. Runs against the
# REPLAY DB (livestreaming_replay); TRUNCATE ... CASCADE handles the
# watchlist_strategies FK so the run always starts from a known clean row.
OUT_SQL = OUT_DIR / "insert_watchlist.sql"

# ================================

LOCAL_TZ = ZoneInfo(TIMEZONE)
UTC = ZoneInfo("UTC")

# IB caps reqHistoricalData at ~2000 seconds per request for 5-sec bars.
# 1800s keeps headroom under the cap.
CHUNK_SECONDS = 1800


def day_bounds(date_str: str):
    day = datetime.strptime(date_str, "%Y-%m-%d")
    start = day.replace(hour=0, minute=0, second=0, tzinfo=LOCAL_TZ)
    end   = start + timedelta(days=1)
    return start, end


def fetch_bars():
    start, end = day_bounds(DATE)

    ib = IB()
    ib.connect(HOST, PORT, clientId=CLIENT_ID)

    contract = Stock(SYMBOL, EXCHANGE, CURRENCY)
    ib.qualifyContracts(contract)

    all_bars = []
    seen_times = set()
    # reqHistoricalData is anchored on endDateTime and pulls `durationStr`
    # backward from there. Anchor each call to the earliest bar we received
    # so we can't skip data, and so post-RTH endpoints (IB clips forward to
    # the RTH close) resolve automatically after the first call.
    chunk_end = end
    call = 0
    while chunk_end > start:
        call += 1
        end_str = chunk_end.astimezone(UTC).strftime("%Y%m%d-%H:%M:%S")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_str,
            durationStr=f"{CHUNK_SECONDS} S",
            barSizeSetting="5 secs",
            whatToShow="TRADES",
            useRTH=USE_RTH,
            formatDate=2,          # tz-aware UTC datetimes
        )

        added = 0
        for b in bars or []:
            # Bound to target day: past pre-market IB falls back to the
            # previous trading day and returns bars that look valid but
            # belong to a different date. Drop them.
            if not (start <= b.date < end):
                continue
            # Adjacent chunks may share a boundary bar; dedupe on timestamp.
            if b.date in seen_times:
                continue
            seen_times.add(b.date)
            all_bars.append(b)
            added += 1

        first_local = last_local = ""
        if bars:
            first_local = bars[0].date.astimezone(LOCAL_TZ).strftime("%H:%M:%S")
            last_local  = bars[-1].date.astimezone(LOCAL_TZ).strftime("%H:%M:%S")
        print(f"  call {call:>3}: +{added:>4} bars  ")

        # Stop when a chunk yields no in-window data (past session start,
        # or IB fell back to another day).
        if added == 0:
            break

        # IB returns bars in ascending time order; anchor the next call to
        # the earliest bar we just received. Dedupe handles the boundary.
        chunk_end = bars[0].date

    ib.disconnect()
    # We walked backward; sort ascending for output.
    all_bars.sort(key=lambda b: b.date)
    return all_bars


def write_csv(bars):
    """
    Same columns as bars_5s.log (%.4f prices/volume/wap, int count) but with
    a header row for readability. The replay reader must skip the header.

    Historical BarData attribute mapping to the live RealTimeBar log:
        bar.average  -> wap
        bar.barCount -> count
    """
    header = "time,symbol,open,high,low,close,volume,wap,count"
  
    with open(OUT_CSV, "w", newline="") as f:
        f.write(header + "\n")
        for b in bars:
            t_local = b.date.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            row = (
                f"{t_local},{SYMBOL},"
                f"{float(b.open):.4f},{float(b.high):.4f},"
                f"{float(b.low):.4f},{float(b.close):.4f},"
                f"{float(b.volume):.4f},{float(b.average):.4f},"
                f"{int(b.barCount)}"
            )
            f.write(row + "\n")



def cleanup_old_csvs() -> None:
    """
    Remove every ``*.csv`` under OUT_DIR before fetching.

    The replay driver picks up every CSV in ``REPLAY_DATA_DIR`` and fans
    them out concurrently; leaving an older run's file behind would mix
    yesterday's symbol/date into today's replay silently. Unconditional
    scrub is safer than trying to match on (SYMBOL, DATE) -- we only ever
    want the file THIS run just produced sitting in the folder.

    OUT_DIR itself is not the source of truth for anything else (the
    fetch script is), so wiping is destructive-but-safe. If you want to
    keep historical fetches around, archive them out of OUT_DIR yourself
    before running.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stale = sorted(OUT_DIR.glob("*.csv"))
    if not stale:
        print(f"cleanup: no stale CSVs in {OUT_DIR}")
        return
    for path in stale:
        try:
            path.unlink()
            print(f"cleanup: removed {path.name}")
        except OSError as e:
            # Fail loud -- a leftover CSV would poison the next replay.
            raise RuntimeError(f"cleanup: failed to remove {path}: {e}") from e


def write_insert_watchlist_sql() -> None:
    """
    Regenerate ``insert_watchlist.sql`` from the current ``SYMBOL`` /
    ``STRATEGY`` so the SQL row you'll run against
    ``livestreaming_replay`` always matches the CSV that was just
    fetched. No manual edits to the SQL file between runs.

    The template mirrors the hand-written version: TRUNCATE both tables
    (CASCADE picks up watchlist_strategies via the FK), reinsert one
    symbol, attach one strategy. ``ON CONFLICT`` clauses stay for
    idempotency if you rerun the SQL manually against a non-truncated DB.
    """
    sql = f"""-- Auto-generated by fetch_bars5s.py -- do not hand-edit.
-- Symbol/strategy are pulled from the fetch script's CONFIG block so
-- this file always matches the CSV in this folder. Runs against the
-- REPLAY database (livestreaming_replay).

-- Wipe exit_requests too: prepare_watchlist() merges armed exits into
-- the monitor set, so a stale row here (e.g. NVDA left over from a
-- previous run) would silently get monitored alongside SYMBOL.
TRUNCATE TABLE exit_requests;
TRUNCATE TABLE watchlist RESTART IDENTITY CASCADE;

WITH w AS (
    INSERT INTO watchlist (symbol)
    VALUES ('{SYMBOL}')
    ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol
    RETURNING id
)
INSERT INTO watchlist_strategies (watchlist_id, strategy_name)
SELECT id, '{STRATEGY}' FROM w
ON CONFLICT (watchlist_id, strategy_name) DO NOTHING;
"""
    OUT_SQL.write_text(sql, encoding="utf-8")
    print(f"Wrote watchlist SQL for {SYMBOL} / {STRATEGY} to {OUT_SQL}")


def main():
    cleanup_old_csvs()
    bars = fetch_bars()
    if not bars:
        print("No bars returned.")
        return
    write_csv(bars)
    print(f"\nSaved {len(bars)} bars to {OUT_CSV}")
    write_insert_watchlist_sql()


if __name__ == "__main__":
    main()
