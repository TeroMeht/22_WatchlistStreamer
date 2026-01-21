import asyncio
import logging
from datetime import date
from src.database.db_functions import insert_order
from src.common.read_configs_in import read_database_config

# Minimal CandleRow class
class CandleRow:
    def __init__(self, symbol, time, date):
        self.symbol = symbol
        self.time = time
        self.date = date

# Helper to create a candle
def make_candle(symbol="TEST", time="12:00", date=date.today()):
    return CandleRow(symbol=symbol, time=time, date=date)

# Simple test: just insert
async def test_insert_order():
    candle = make_candle("BTC_TEST", "12:00")
    stoplevel = 999.99

    # Load real database configuration
    database_config = read_database_config(filename="database.ini", section="livestream")
    print("Using database config:", database_config)

    try:
        # Call the actual function
        await insert_order(candle, stoplevel, database_config)
        print("Insert attempted successfully!")
    except Exception as e:
        print("Insert failed with exception:", e)

# Run manually
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  # enable logging
    asyncio.run(test_insert_order())