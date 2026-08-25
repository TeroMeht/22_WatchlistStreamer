import pandas as pd
import logging
from typing import Optional, List,Dict
from src.database.db_functions import *


# Tämä on erillinen koodikirjasto jolla käsittelen sisään tulevia bars dataa pandas dataframeiksi
logger = logging.getLogger(__name__)  # module-specific logger

from dataclasses import asdict
from zoneinfo import ZoneInfo

from src.core.config import settings
from data_sources._bar import IncomingBar

from indicators.rvol import avg_volume_model
from indicators.vwap import vwap_series
from indicators.ema import ema_series
from indicators.atr import atr_series
from indicators.relatr import relatr_series
from indicators.day_atr_ext import day_atr_ext_series


from dataclasses import asdict
from src.helpers.handle_candles import CandleRow
from src.streamer.session_state import SymbolSessionState

def intraday_datapipe(bars: List[IncomingBar]) -> pd.DataFrame:
    """
    Convert a list of IncomingBar dataclasses to a pandas DataFrame.
    Keeps datetime as timezone-aware and capitalizes all column names.
    """
    time_zone = settings.TIMEZONE
    # Convert dataclasses to DataFrame
    df = pd.DataFrame([asdict(bar) for bar in bars])

    # Drop optional columns if present
    df = df.drop(columns=[c for c in ["average", "barCount"] if c in df.columns])

    # --- Convert datetime to Helsinki timezone ---
    df["date"] = pd.to_datetime(df["date"], utc=True)  # treat all as UTC
    df["date"] = df["date"].dt.tz_convert(ZoneInfo(time_zone))  # convert to Helsinki coming from project config

    # --- Split Date / Time for readability ---
    df["date"] = df["date"].dt.date      # keep only date part
    df["time"] = df["date"].dt.time      # optional: separate Time column

    # Drop original 'date' column if you want
    df = df.drop(columns=["date"])

    # Capitalize all remaining column names
    df.columns = [col.capitalize() for col in df.columns]

    return df

def daily_datapipe(bars: List[IncomingBar]) -> pd.DataFrame:
    """
    Convert a list of IncomingBar dataclasses to a pandas DataFrame.
    Keeps datetime as timezone-aware and capitalizes all column names.
    """

    # Convert dataclasses to DataFrame
    df = pd.DataFrame([asdict(bar) for bar in bars])

    # Capitalize all remaining column names
    df.columns = [col.capitalize() for col in df.columns]

    return df




def handle_incoming_dataframe_daily(bars: List[IncomingBar], symbol:str)-> pd.DataFrame:


    incoming_bars = IncomingBar.from_raw_bars(bars)

    df = daily_datapipe(incoming_bars)
    
    df['symbol'] = symbol


    # Calculate ATR (assumes this function adds Prev_Close, TR, ATR columns)
    df = atr_series(df)

    # --- Reorder columns ---
    desired_order = [
        "symbol","date", "open", "high", "low", "close", "volume",
        "Average", "BarCount", "prev_close", "tr", "atr"
    ]
    # Keep only columns that exist (some may be missing)
    df = df[[col for col in desired_order if col in df.columns]]


    return df

def handle_incoming_dataframe_intradays_volume(bars: List[IncomingBar], symbol:str)-> pd.DataFrame:

    # Step 1: Convert to dataclasses
    incoming_bars = IncomingBar.from_raw_bars(bars)

    # Step 2: Convert to DataFrame
    df = intraday_datapipe(incoming_bars)

    # Step 4: Assign symbol
    df["symbol"] = symbol
            # Step 4: Ensure Date column is datetime
    df["date"] = pd.to_datetime(df["date"]).dt.date


    from src.streamer.replay import get_effective_today
    today = get_effective_today()

    df_today = df[df["date"] == today].copy()
    df_past = df[df["date"] != today].copy()
    # Keep only rows with time >= 11:00
    df_today = df_today[df_today["time"] >= time(11, 0)]


    df_past = df_past[["symbol","date","time","open","high","low","close","volume"]]

    # Step 5: Calculate average volume model
    df_past = avg_volume_model([df_past])
    df_today = vwap_series(df_today)
    df_today = ema_series(df_today)

    df_today = df_today[[
        "symbol","date","time","open","high","low","close","volume","vwap","ema9"
    ]]
    return df_today,df_past


# ---------------------------------------------------------------------------
# Focused pipeline transforms -- one function per output frame.
# Used by the IB three-way fetch path (see datastreamer._fetch_history_data).
# ---------------------------------------------------------------------------


def bars_to_today_frame(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    incoming_bars = IncomingBar.from_raw_bars(bars)
    df = intraday_datapipe(incoming_bars)
    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"]).dt.date

    df = df[df["time"] >= time(11, 0)]

    df = vwap_series(df)
    df = ema_series(df)

    return df[[
        "symbol", "date", "time", "open", "high", "low",
        "close", "volume", "vwap", "ema9",
    ]]


def bars_to_avg_volume_frame(bars: List[IncomingBar], symbol: str) -> pd.DataFrame:

    incoming_bars = IncomingBar.from_raw_bars(bars)
    df = intraday_datapipe(incoming_bars)
    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"]).dt.date

    df = df[["symbol", "date", "time", "open", "high", "low", "close", "volume"]]

    return avg_volume_model([df])



def build_last_atr_dict(daily_with_atr: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """Latest ATR per symbol from the daily-bars dict keyed by Symbol."""
    return {symbol: df['atr'].iloc[-1] for symbol, df in daily_with_atr.items()}


def build_last_prev_close_dict(daily_with_atr: Dict[str, pd.DataFrame]) -> Dict[str, float]:

    return {symbol: float(df['close'].iloc[-1]) for symbol, df in daily_with_atr.items()}
# ---------------------------------------------------------------------------
# Warmup enrichment via SessionStore.apply_bar
#
# Replaces the two prior batch functions (handle_intraday_rvol_dataset +
# handle_Atr_intraday_dataset) with one walk-per-symbol that mirrors
# the live path: same ``apply_bar`` code produces the primed bars at
# boot AND enriches each incoming bar at runtime. Any drift between
# warmup and live is therefore impossible -- there is only one code
# path.
# ---------------------------------------------------------------------------


def seed_and_enrich_intraday(
    store,                                  # src.streamer.session_state.SessionStore
    today_intra:  dict[str, pd.DataFrame],
    past_intra:   dict[str, pd.DataFrame],
    last_atr_per_symbol:        dict[str, float],
    last_prev_close_per_symbol: dict[str, float],
) -> dict[str, pd.DataFrame]:




    cols_order = [
        'symbol', 'date', 'time', 'open', 'high', 'low', 'close',
        'volume', 'vwap', 'ema9', 'avg_volume', 'rvol', 'relatr', 'day_atr_ext'
    ]

    enriched: dict[str, pd.DataFrame] = {}

    for symbol, today_df in today_intra.items():
        # Session date = earliest bar's date in the frame (they're all
        # the same day per warmup contract).
        session_date = today_df['date'].iloc[0] if not today_df.empty else None
        st: SymbolSessionState = store.init(symbol, session_date)
        st.atr        = last_atr_per_symbol.get(symbol)
        st.prev_close = last_prev_close_per_symbol.get(symbol)

        past_df = past_intra.get(symbol)
        if past_df is not None and not past_df.empty:
            st.rvol_baseline = dict(zip(past_df['time'], past_df['avg_volume']))
        else:
            st.rvol_baseline = {}

        rows_out = []
        for _, r in today_df.iterrows():
            candle = CandleRow(
                symbol      = symbol,
                date        = r['date'],
                time        = r['time'],
                open        = float(r['open']),
                high        = float(r['high']),
                low         = float(r['low']),
                close       = float(r['close']),
                volume      = float(r['volume']),
                vwap        = None,
                ema9        = None,
                avg_volume  = None,
                rvol        = None,
                relatr      = None,
                day_atr_ext = None,
            )
            st.apply_bar(candle)
            d = asdict(candle)
            # Map 22 candle-field names -> DB column names.
            rows_out.append({
                'symbol':     d['symbol'],
                'date':       d['date'],
                'time':       d['time'],
                'open':       d['open'],
                'high':       d['high'],
                'low':        d['low'],
                'close':      d['close'],
                'volume':     d['volume'],
                'vwap':       d['vwap'],
                'ema9':       d['ema9'],
                'avg_volume': d['avg_volume'],
                'rvol':       d['rvol'],
                'relatr':     d['relatr'],
                'day_atr_ext':  d['day_atr_ext'],
            })

        out_df = pd.DataFrame(rows_out, columns=cols_order)
        enriched[symbol] = out_df
        logger.debug(f"{symbol} - last 10 rows:\n{out_df.tail(10)}")

    return enriched
