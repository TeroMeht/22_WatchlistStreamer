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
SYMBOL      = "AMLX"
DATE        = "2026-08-18"          # YYYY-MM-DD (local calendar date)
EXCHANGE    = "SMART"
CURRENCY    = "USD"

HOST        = "127.0.0.1"
PORT        = 4002                  # 7497 paper TWS, 7496 live TWS, 4002/4001 gateway
CLIENT_ID   = 20

USE_RTH     = True                  # True = regular trading hours only
TIMEZONE    = "Europe/Helsinki"     # output timestamp tz (matches settings.TIMEZONE)

# Hard-coded output file
OUT_CSV = Path(
    fr"C:\codebase\prod\22_WatchlistStreamer\replay\{SYMBOL}_{DATE}_5s.csv"
)

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



def main():
    bars = fetch_bars()
    if not bars:
        print("No bars returned.")
        return
    write_csv(bars)
    print(f"\nSaved {len(bars)} bars to {OUT_CSV}")


if __name__ == "__main__":
    main()
