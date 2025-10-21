from src.alarm_logics import *

import logging


logger = logging.getLogger(__name__)





def reversal_strategy(candle, database_config, project_config):
    """
    Reversal long strategy:
    - detect capitulation
    - trigger EMA9 crossover up
    """
    symbol = candle[0].lower()
    logger.info("Running Reversal Long strategy for symbol: %s", candle[0])

    capitulation = detect_capitulation(
        symbol,
        num_rows=5,
        database_config=database_config,
        threshold=project_config["capitulation_threshold"]
    )

    if capitulation:
        logger.info("Capitulation detected for symbol: %s. Triggering EMA9 crossover up check.", symbol)
        detect_ema9crossover_up(
            symbol,
            num_rows=2,
            database_config=database_config,
            project_config=project_config
        )
    else:
        pass


def reversal_short_strategy(candle, database_config, project_config):
    """
    Reversal short strategy:
    - detect euforia
    - trigger EMA9 crossover down
    """
    symbol = candle[0].lower()
    logger.info("Running Reversal Short strategy for symbol: %s", candle[0])

    euforia = detect_euforia(
        symbol,
        num_rows=5,
        database_config=database_config,
        threshold=project_config["capitulation_threshold"]
    )

    if euforia:
        logger.info("Euforia detected for symbol: %s. Triggering EMA9 crossover down check", symbol)
        detect_ema9crossover_down(
            symbol,
            num_rows=2,
            database_config=database_config,
            project_config=project_config
        )
    else:
        pass



def vwapcontinuation_strategy(candle, database_config, project_config):
    """
    VWAP Continuation strategy:
    - detect euforia
    - trigger VWAP closeness check
    """
    symbol = candle[0].lower()
    logger.info("Running VWAP Continuation strategy for symbol: %s", candle[0])


    euforia= detect_euforia_all(
                table_name=symbol,
                database_config=database_config,
                threshold=project_config["capitulation_threshold"]
            )
    if euforia:
        detect_vwap_closeness(
            table_name=symbol,
            num_rows=1,
            database_config=database_config,
            vwap_distance=project_config["vwap_distance"],
            project_config=project_config
        )
 


    # if euforia:
    #     logger.info("Euforia detected for symbol: %s. Triggering EMA9 crossover up check", symbol)
    #     detect_ema9crossover_up(
    #         symbol,
    #         num_rows=2,
    #         database_config=database_config,
    #         project_config=project_config
    #     )
    else:
        pass








def exit_strategy(candle, database_config, project_config):
    """
    Exit strategy:
    - Trigger EMA9 crossover down for exiting positions
    """
    symbol = candle[0].lower()
    logger.info("Running Exit strategy for symbol: %s", candle[0])

    
    detect_ema9crossover_down(
        symbol,
        num_rows=2,
        database_config=database_config,
        project_config=project_config
    )


def run_strategies(project_config,
                   database_config, 
                   last_candle
                   ):
    

    """Run all trading strategies on the finalized candle."""


    reversal_strategy(last_candle, database_config, project_config)
    # vwapcontinuation_strategy(last_candle, database_config, project_config)
    # reversal_short_strategy(last_candle, database_config, project_config)  
        #exit_strategy(last_candle, database_config, project_config)
    