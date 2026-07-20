"""
Standalone datapipe validator.

Runs the exact same fetch + transform pipeline as run_streamer() for a hand-
picked list of symbols, then dumps every intermediate and final DataFrame to
CSV so you can eyeball the numbers (VWAP, EMA9, Avg_volume, Rvol, Relatr)
without spinning up the DB or the live monitor.

Usage:
    python test_datapipe.py                # uses SYMBOLS default below
    python test_datapipe.py AAPL MSFT NVDA # override from CLI

Outputs land in ./datapipe_csv/<timestamp>/ as:
    <SYMBOL>_daily.csv          # 14-day daily bars + ATR
    <SYMBOL>_avg_volume.csv     # per-time-of-day avg volume (past 4 days)
    <SYMBOL>_intraday_final.csv # full 5-day 2-min bars + VWAP/EMA9/Rvol/Relatr
    last_atr.csv                # symbol -> last ATR used for Relatr

Uses a different IB client ID than the main streamer so you can run both at once.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from ib_async import IB

from src.common.logging_config import setup_logging
from src.core.config import settings
from src.helpers.ibclient import fetch_history_daily, fetch_intraday_volume_history
from src.helpers.handle_dataframes import (
    handle_intraday_rvol_dataset,
    handle_Atr_intraday_dataset,
)


SYMBOLS_DEFAULT = ["AAPL", "MSFT"]
CLIENT_ID_OFFSET = 100  # avoid clashing with the live streamer


def _outdir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path("datapipe_csv") / stamp
    out.mkdir(parents=True, exist_ok=True)
    return out


async def run(symbols: list[str]) -> None:
    out = _outdir()
    logging.info("Writing CSVs to %s", out.resolve())

    ib = IB()
    await ib.connectAsync(
        settings.IB_HOST,
        settings.IB_PORT,
        clientId=settings.IB_CLIENT_ID + CLIENT_ID_OFFSET,
    )

    try:
        # --- Fetch in parallel, same shape as run_streamer ---
        tasks = []
        for sym in symbols:
            tasks.append(fetch_history_daily(ib, sym))
            tasks.append(fetch_intraday_volume_history(ib, sym))

        results = await asyncio.gather(*tasks)
        daily_data = results[0::2]
        intraday_data = results[1::2]

        today_intradaydata = [r[0] if r else None for r in intraday_data]
        past_intradaydata = [r[1] if r else None for r in intraday_data]

        # --- Run the same transforms as run_streamer ---
        rvol_dataset = handle_intraday_rvol_dataset(
            today_intradaydata, past_intradaydata
        )
        relatr_datasets, last_atr_dict = handle_Atr_intraday_dataset(
            rvol_dataset, daily_data
        )

        # --- Dump everything ---
        # Daily + ATR per symbol
        for df in daily_data:
            if df is None or df.empty:
                continue
            sym = df["Symbol"].iloc[0]
            df.to_csv(out / f"{sym}_daily.csv", index=False)

        # Avg-volume model per symbol (past 4 days, per time-of-day)
        for df in past_intradaydata:
            if df is None or df.empty:
                continue
            sym = df["Symbol"].iloc[0]
            df.to_csv(out / f"{sym}_avg_volume.csv", index=False)

        # Final intraday: 5 days of 2-min bars with VWAP/EMA9/Rvol/Relatr
        for sym, df in relatr_datasets.items():
            if df is None or df.empty:
                continue
            df.to_csv(out / f"{sym}_intraday_final.csv", index=False)
            logging.info(
                "%s -> intraday_final rows=%d, dates=%s",
                sym,
                len(df),
                sorted(df["Date"].unique().tolist()),
            )

        # Last ATR lookup
        import pandas as pd

        pd.DataFrame(
            [{"Symbol": s, "ATR": a} for s, a in last_atr_dict.items()]
        ).to_csv(out / "last_atr.csv", index=False)

        logging.info("Done. %d symbols processed.", len(relatr_datasets))

    finally:
        ib.disconnect()


def _parse_args() -> list[str]:
    p = argparse.ArgumentParser(description="Validate the watchlist datapipe to CSV.")
    p.add_argument(
        "symbols",
        nargs="*",
        help=f"Symbols to test (default: {' '.join(SYMBOLS_DEFAULT)})",
    )
    args = p.parse_args()
    return [s.upper() for s in (args.symbols or SYMBOLS_DEFAULT)]


if __name__ == "__main__":
    setup_logging()
    syms = _parse_args()
    logging.info("Testing symbols: %s", syms)
    try:
        asyncio.run(run(syms))
    except KeyboardInterrupt:
        sys.exit(130)
