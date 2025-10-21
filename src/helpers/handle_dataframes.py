from src.common.calculate import *
import pandas as pd
from src.common.adjust_timezone import adjust_timezone_IB_data
import logging

# Tämä on erillinen koodikirjasto jolla käsittelen sisään tulevia bars dataa pandas dataframeiksi
logger = logging.getLogger(__name__)  # module-specific logger


def handle_incoming_dataframe_intraday(bars, symbol):
    """
    Process IBKR historical bars list into a DataFrame, adjust timezone, calculate VWAP/EMA9.
    `bars` can be a list of RealTimeBar or BarData objects.
    """
    try:
        if bars is not None and len(bars) > 0:

            # Convert bars to DataFrame and adjust timezone
            data_list = []
            for bar in bars:
                adjusted_time = adjust_timezone_IB_data(bar.date)
                data_list.append({
                    'date': adjusted_time,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'average': getattr(bar, 'average', None),
                    'barCount': getattr(bar, 'barCount', None)
                })

            bars_df = pd.DataFrame(data_list)

            # Drop unnecessary columns
            for col in ['average', 'barCount']:
                if col in bars_df.columns:
                    bars_df = bars_df.drop(columns=[col])

            bars_df.columns = [col.capitalize() for col in bars_df.columns]
            bars_df['Symbol'] = symbol
            bars_df = bars_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Calculate indicators
            bars_df = calculate_vwap(bars_df)
            bars_df = calculate_ema(bars_df, period=9)

            # Split Date into Date and Time
            if bars_df['Date'].dtype == object:
                bars_df[['Date', 'Time']] = bars_df['Date'].str.split(' ', expand=True)
            else:
                bars_df['Date'] = bars_df['Date'].astype(str)
                bars_df[['Date', 'Time']] = bars_df['Date'].str.split(' ', expand=True)

            data = bars_df[['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'VWAP', 'EMA9']].copy()
            return data

        else:
            logging.warning("empty data")
            return None

    except Exception as e:
        logging.error(f"An error occurred while processing the data: {e}")
        return None


def handle_incoming_dataframe_daily(bars, symbol):
    if not bars:
        logging.warning("Empty data")
        return None

    # Convert bars to DataFrame
    data_list = []
    for bar in bars:
        data_list.append({
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        })

    bars_df = pd.DataFrame(data_list)
    bars_df.columns = [col.capitalize() for col in bars_df.columns]
    bars_df['Symbol'] = symbol
    bars_df = bars_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # Calculate ATR for all rows and return full DataFrame
    bars_df = calculate_14day_atr_df(bars_df)
    logger.info(bars_df.tail(10))
    return bars_df


def handle_Atr_intraday_dataset(symbols, intraday_results, daily_results):
    """
    Calculate Relatr for each intraday dataset and return in the same format
    as handle_incoming_dataframe_intraday (including Symbol, Date, Time, Open,
    High, Low, Close, Volume, VWAP, EMA9, Relatr).
    """
    relatr_datasets = {}

    for i, symbol in enumerate(symbols):
        intraday_df = intraday_results[i]
        daily_df = daily_results[i]

        if intraday_df is not None and daily_df is not None:
            # Calculate Relatr using the ATR14 from daily data
            intraday_df = calculate_relatr(intraday_df, daily_df)

            # Ensure column order matches desired format
            cols_order = ['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 
                          'Volume', 'VWAP', 'EMA9', 'Relatr']
            # Add missing columns if needed
            for col in cols_order:
                if col not in intraday_df.columns:
                    intraday_df[col] = None

            intraday_df = intraday_df[cols_order]

            relatr_datasets[symbol] = intraday_df

            # Print last 10 rows for verification
            logger.info(intraday_df.tail(10))
        else:
            logger.error(f"Data missing for {symbol}, skipping Relatr calculation.")
            relatr_datasets[symbol] = intraday_df  # keep original if one is None

    return relatr_datasets