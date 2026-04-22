from src.alarms.alarm_logics import *
from src.alarms.alarm_generator import generate_signal_alarm
from src.database.db_functions import get_last_rows
import asyncio
import logging
from src.helpers.utils import *
from src.alarms.send_postrequest import send_alarm_to_fastapi, send_exit_request_to_fastapi
from config import CLIENT_CONFIG

logger = logging.getLogger(__name__)




async def euforia_exit_strategy(candle: CandleRow):

    logger.info("Running Euforia Exit for long symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="euforia",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )



async def endofday_exit_strategy(candle: CandleRow):
    logger.info("Running End of Day Exit for symbol: %s", candle.symbol)

    await send_exit_request_to_fastapi(
        candle=candle,
        alarm_name="endofday_exit",
        fastapi_url=CLIENT_CONFIG["exit-request_endpoint"]
    )

async def reversal_strategy(last_8_rows:pd.DataFrame, candle: CandleRow):

    logger.info("Running Reversal Long strategy for symbol: %s | RelATR: %.4f",
    candle.symbol,
    candle.relatR)

    # Check for capitulation using the DataFrame
    if detect_capitulation(last_8_rows, threshold=CLIENT_CONFIG["capitulation_threshold"]):
        logger.debug("Capitulation detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)
        
        if is_crossover_up(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(last_8_rows, direction="long")
            logging.info(
                f"{candle.symbol}: Stop level set at {stop_level}"
            )

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover up",
                stop_level=stop_level,
                project_config=CLIENT_CONFIG
            )

async def reversal_short_strategy(last_8_rows:pd.DataFrame, candle: CandleRow):

    logger.info("Running Reversal Short strategy for symbol: %s | RelATR: %.4f",
    candle.symbol,
    candle.relatR)

    # Check for euforia using the DataFrame
    if detect_euforia(last_8_rows, threshold=CLIENT_CONFIG["capitulation_threshold"]):
        logger.debug("Euforia detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_down(last_8_rows.tail(2)):
            last_row = last_8_rows.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover down detected, generating alarm...")

            # --- Calculate stop level ---
            stop_level = detect_stoplevel(last_8_rows, direction="short")
            logging.info(
                f"{candle.symbol}: Stop level set at {stop_level}"
            )

            # Generate signal alarm with stop level
            await generate_signal_alarm(
                candle=candle,
                signal_name="EMA9 crossover down",
                stop_level=stop_level,
                project_config=CLIENT_CONFIG
            )



# async def vwapcontinuation_strategies(past_dataSet:pd.DataFrame, candle: CandleRow):

#     logger.info("Running VWAP Continuation strategies for symbol: %s", candle.symbol)

#     if is_vwap_close(candle, CLIENT_CONFIG["vwap_distance"]): # Jos hinta on lähellä VWAP niin tarkasta mistä se on tullut

#         # Check VWAP closeness first (latest row)
#         df_all = past_dataSet
#         # Tarvii tarkastaa että onhan hinta ollut viimeaikoina VWAP yläpuolella
#         last_5 = df_all.tail(5)
#         avg_relatr = last_5["Relatr"].mean() # lasketaan viimeisimpien 5 candlejen relatr keskiarvo. Tämän etumerkin perusteella päätellään missä hinta on ollut

#         # Tarkoituksena käyttää samaa kannasta haettua dataa jottei sitä tarvi kysellä uudelleen
#         if avg_relatr < 0:  # jos tää on ollut pienempi kuin 0 niin on oltu VWAP yläpuolella
#             # Log the average Relatr for debugging/visibility
#             logger.info(
#                 "Price has been above VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
#                 candle.symbol,
#                 avg_relatr
#             )
#             # Detect euforia
#             if detect_euforia(df_all, threshold=CLIENT_CONFIG["capitulation_threshold"]):
#                 logger.info(f"Euforia detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

#                 await generate_signal_alarm(candle=candle,
#                                             signal_name="VWAP continuation setup",
#                                             project_config=CLIENT_CONFIG)
            
#         elif avg_relatr > 0: #jos tää on ollut suurempi kuin 0 niin on oltu VWAP alapuolella
#             logger.info(
#                 "Price has been below VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
#                 candle.symbol,
#                 avg_relatr
#             )
#             # Detect capitulation
#             # if detect_capitulation(df_all, threshold=project_config["capitulation_threshold"]):
#             #     logger.info(f"Capitulation detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

#             #     await generate_signal_alarm(candle=candle,
#             #                                 signal_name="VWAP continuation short setup",
#             #                                 database_config=database_config,
#             #                                 project_config=project_config)
#     else:
#         logger.debug("Average Relatr is neutral for symbol: %s, not near VWAP.", candle.symbol)

async def downside_extension(candle: CandleRow):

    logger.info(f"Capitulation alarm: {candle.symbol} with Relatr: {candle.relatR:.3f}")

    await generate_signal_alarm(candle=candle,
                                signal_name= f"Capitulation the downside with Relatr: {candle.relatR:.3f}",
                                stop_level=None, # No stop level for this strategy                                 
                                project_config=CLIENT_CONFIG)


async def upside_extension(candle: CandleRow):
    logger.info(f"Euforic extension detected for symbol: {candle.symbol} with Relatr: {candle.relatR:.3f}")

    await generate_signal_alarm(candle=candle,
                                signal_name= f"Euforic extension the upside with Relatr: {candle.relatR:.3f}",
                                stop_level=None,                                 
                                project_config=CLIENT_CONFIG)


async def run_strategies(candle: CandleRow):


    # Prepare a list of coroutines to run
    tasks = []

    # Thresholds — adjust to your logic
    # if candle.relatR > 0 and candle.rvol >= 1.5: # ei ole riittävän in play jos Rvol jää tämän alapuolelle
    #         # Hae data tältä sisääntulleelta kynttilältä
    #     last_8_rows = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)
    #     tasks.append(reversal_strategy(last_8_rows, candle))

    # if candle.relatR < 0 and candle.rvol >= 1.5:
    #         # Hae data tältä sisääntulleelta kynttilältä
    #     last_8_rows = await get_last_rows(table_name=f"{candle.symbol.lower()}_livestream", num_rows=8)
    #     tasks.append(reversal_short_strategy(last_8_rows, candle))

    # if candle.relatR >= CLIENT_CONFIG["capitulation_threshold"]:
    #     tasks.append(capitulation_exit_strategy(candle))

    if candle.relatR >= CLIENT_CONFIG["capitulation_threshold"] and candle.rvol >= 1.5: 
        tasks.append(downside_extension(candle))

    if candle.relatR <= -CLIENT_CONFIG["capitulation_threshold"] and candle.rvol >= 1.5: 
         tasks.append(upside_extension(candle))

    if candle.relatR <= -CLIENT_CONFIG["capitulation_threshold"]:
        tasks.append(euforia_exit_strategy(candle))
        
    if candle.time >= CLIENT_CONFIG["endofday"]:
        tasks.append(endofday_exit_strategy(candle))

    # Run all selected strategies concurrently
    if tasks:
        await asyncio.gather(*tasks)
