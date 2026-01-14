from src.alarms.alarm_logics import *
from src.alarms.alarm_generator import generate_signal_alarm
from src.database.db_functions import get_last_rows
import asyncio
import logging
from src.helpers.utils import *


logger = logging.getLogger(__name__)





async def reversal_strategy(past_dataSet:pd.DataFrame, candle: CandleRow, project_config:dict, database_config:dict):


    logger.info("Running Reversal Long strategy for symbol: %s", candle.symbol)

    #Otetaan viimeiset 5 riviä datasta tarkastelua varten
    df = past_dataSet.tail(5)

    # Check for capitulation using the DataFrame
    if detect_capitulation(df, threshold=project_config["capitulation_threshold"]):
        logger.debug("Capitulation detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)
        
        if is_crossover_up(df.tail(2)):
            last_row = df.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover up detected, generating alarm...")

            await generate_signal_alarm(candle=candle,
                                        signal_name="EMA9 crossover up",
                                        database_config=database_config,
                                        project_config=project_config)
        logger.info("Running Reversal Long strategy for symbol: %s", candle.symbol)


    # Check for euforia using the DataFrame
    if detect_euforia(df, threshold=project_config["capitulation_threshold"]):
        logger.debug("Euforia detected for symbol: %s. Checking EMA9 crossover...", candle.symbol)

        if is_crossover_down(df.tail(2)):
            last_row = df.iloc[-1]
            logging.info(f"{last_row['Symbol']}: EMA9 crossover down detected, generating alarm...")

            await generate_signal_alarm(candle=candle,
                                        signal_name="EMA9 crossover down",
                                        database_config=database_config,
                                        project_config=project_config)        


    else:
        logger.info("No capitulation or euforia within 5 candles were detected for symbol: %s", candle.symbol)

async def vwapcontinuation_strategies(past_dataSet:pd.DataFrame, candle: CandleRow, project_config:dict, database_config:dict):

    logger.info("Running VWAP Continuation strategies for symbol: %s", candle.symbol)

    if is_vwap_close(candle, project_config["vwap_distance"]): # Jos hinta on lähellä VWAP niin tarkasta mistä se on tullut

        # Check VWAP closeness first (latest row)
        df_all = past_dataSet
        # Tarvii tarkastaa että onhan hinta ollut viimeaikoina VWAP yläpuolella
        last_5 = df_all.tail(5)
        avg_relatr = last_5["Relatr"].mean() # lasketaan viimeisimpien 5 candlejen relatr keskiarvo. Tämän etumerkin perusteella päätellään missä hinta on ollut

        # Tarkoituksena käyttää samaa kannasta haettua dataa jottei sitä tarvi kysellä uudelleen
        if avg_relatr < 0:  # jos tää on ollut pienempi kuin 0 niin on oltu VWAP yläpuolella
            # Log the average Relatr for debugging/visibility
            logger.info(
                "Price has been above VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
                candle.symbol,
                avg_relatr
            )
            # Detect euforia
            if detect_euforia(df_all, threshold=project_config["capitulation_threshold"]):
                logger.info(f"Euforia detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

                await generate_signal_alarm(candle=candle,
                                            signal_name="VWAP continuation setup",
                                            database_config=database_config,
                                            project_config=project_config)
            
        elif avg_relatr > 0: #jos tää on ollut suurempi kuin 0 niin on oltu VWAP alapuolella
            logger.info(
                "Price has been below VWAP recently for symbol: %s | avg Relatr of last 5 candles: %.4f",
                candle.symbol,
                avg_relatr
            )
            # Detect capitulation
            # if detect_capitulation(df_all, threshold=project_config["capitulation_threshold"]):
            #     logger.info(f"Capitulation detected earlier for symbol: {candle.symbol} now near VWAP, triggering VWAP setup alarm...")

            #     await generate_signal_alarm(candle=candle,
            #                                 signal_name="VWAP continuation short setup",
            #                                 database_config=database_config,
            #                                 project_config=project_config)
    else:
        logger.debug("Average Relatr is neutral for symbol: %s, not near VWAP.", candle.symbol)

async def extreme_extension(candle: CandleRow, project_config:dict, database_config:dict):

    # Tää ei tarvi historia dataa koska tarkastetaan vain sisään tulleen candle:n relatr arvoa
    # downside alarm 
    if candle.relatR >= project_config["extreme_extension_threshold"]: #suurempi tai yhtä suuri kuin 1 tarkoittaa downsidea
        logger.info(f"Extreme Extension detected for symbol: {candle.symbol} with Relatr: {candle.relatR:.3f}")

        await generate_signal_alarm(candle=candle,
                                    signal_name= "Extreme Extension to downside",
                                    database_config=database_config,
                                    project_config=project_config)
   # upside alarm
    elif candle.relatR <= -project_config["extreme_extension_threshold"]:
        logger.info(f"Extreme Extension detected for symbol: {candle.symbol} with Relatr: {candle.relatR:.3f}")

        await generate_signal_alarm(candle=candle,
                                    signal_name= "Extreme Extension to upside",
                                    database_config=database_config,
                                    project_config=project_config)
    else:
        pass





async def run_strategies(candle: CandleRow, project_config :dict, database_config:dict):
    past_Dataset = pd.DataFrame()
    # Hae kaikki data tältä sisääntulleelta kynttilältä
    past_Dataset = await get_last_rows(table_name=candle.symbol.lower(), num_rows=None, database_config=database_config)


    """Run all trading strategies on the finalized candle."""
    await asyncio.gather(#reversal_strategy(past_Dataset, candle, project_config,database_config),
                        # vwapcontinuation_strategies(past_Dataset, candle, project_config, database_config),
                         extreme_extension(candle, project_config, database_config))
