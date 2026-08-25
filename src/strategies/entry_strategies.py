from src.alarms.alarm_logics import *
from src.database.db_functions import get_last_rows
import logging
from src.helpers.utils import *
from src.alarms.alarm_generator import generate_signal_alarm
from src.orders.order_generator import generate_entry_order, detect_stoplevel
from src.core.config import settings

logger = logging.getLogger(__name__)




# =============================================================================
# Strategy implementations
# =============================================================================


async def reversal_strategy(candle: CandleRow):

    logger.info("Running Reversal Long strategy for symbol: %s | RelATR: %.4f", candle.symbol, candle.relatr)

    df_all = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)

    if is_crossover_up(df_all.tail(2)):
        last_row = df_all.iloc[-1]
        logging.info(f"{last_row['symbol']}: EMA9 crossover up detected")

        # Check for capitulation using the DataFrame
        if detect_capitulation(df_all, threshold=settings.CAPITULATION_THRESHOLD):
            logger.debug("Capitulation detected for symbol: %s. Generating alarm", candle.symbol)

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(df_all, direction="long")

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover up"
            )

            await generate_entry_order(
                candle=candle,
                stop_level=stop_level
            )


async def reversal_short_strategy(candle: CandleRow):

    logger.info("Running Reversal Short strategy for symbol: %s | RelATR: %.4f", candle.symbol, candle.relatr)

    df_all = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)

    if is_crossover_down(df_all.tail(2)):
        last_row = df_all.iloc[-1]
        logging.info(f"{last_row['symbol']}: EMA9 crossover down detected")

        # Check for euforia using the DataFrame
        if detect_euforia(df_all, threshold=settings.EUFORIC_THRESHOLD):
            logger.debug("Euforia detected for symbol: %s. Generating alarm", candle.symbol)

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(df_all, direction="short")
            logging.info(f"{candle.symbol}: Stop level set at {stop_level}")

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover down"
            )
            await generate_entry_order(
                candle=candle,
                stop_level=stop_level
            )


async def vwapcontinuation_short_strategy(candle: CandleRow):

    logger.info("Running VWAP Continuation short strategy for symbol: %s", candle.symbol)

    if candle.rvol >= settings.RVOL_THRESHOLD:
        # Jos sisääntullut kynttilä on lähellä VWAPia, tarkastetaan onko viimeaikoina ollut euforiaa
        if is_vwap_close(candle, settings.VWAP_DISTANCE):

        # Hakee rivit session alusta asti, jotta tiedetään tapahtuiko euforia jossain kohtaa
            df_all = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=None)


            # Tarvii tarkastaa että onhan hinta ollut viimeaikoina VWAP yläpuolella
            last_5 = df_all.tail(5)
            avg_relatr = last_5["relatr"].mean()  # lasketaan viimeisimpien 5 candlejen relatr keskiarvo. Tämän etumerkin perusteella päätellään missä hinta on ollut


            if avg_relatr > 0: # to the downside
                # Log the average Relatr for debugging/visibility
                logger.info(
                    "Price has been below VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
                    candle.symbol,
                    avg_relatr,
                )
                # Detect earlier capitulation
                if detect_capitulation(df_all, threshold=settings.CAPITULATION_THRESHOLD):
                    logger.info(f"Capitulation detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

                    await generate_signal_alarm(candle=candle,
                                                signal_name="VWAP continuation short setup")
    else:
        logger.info("VWAP Continuation short strategy skipped for symbol: %s | RelATR: %.4f < %.4f", candle.symbol, candle.relatr, settings.RVOL_THRESHOLD)
