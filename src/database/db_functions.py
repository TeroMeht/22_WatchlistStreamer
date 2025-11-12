import psycopg2
import pandas as pd
from decimal import Decimal
from src.common.calculate import *
from src.helpers.handle_candles import *

from datetime import datetime, timedelta
import logging

import asyncpg
from src.helpers.utils import *

logger = logging.getLogger(__name__)  # module-specific logger


def get_connection_and_cursor(database_config):
    """Create and return a database connection and cursor."""
    conn = psycopg2.connect(**database_config)
    if not conn:
        raise Exception("Failed to connect to database.")
    cur = conn.cursor()
    return conn, cur

def delete_all_tables_db(database_config):
    conn = None
    cur = None
    try:
        # Check database name first
        db_name = database_config.get("database")
        if db_name != "livestreaming" and db_name != "volumemodels":
            logging.info(f"Aborting: database is '{db_name}', not 'livestreaming' or 'volumemodels'.")
            return

        conn, cur = get_connection_and_cursor(database_config)
        # Fetch all table names
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
        """)
        tables = cur.fetchall()

        if not tables:
            logging.warning("No tables found in the database.")
            return

        # Disable foreign key checks
        cur.execute("SET session_replication_role = replica;")

        # Drop each table except 'alarms'
        for table in tables:
            table_name = table[0]
            if table_name == "alarms" or table_name== "livedata":
                logging.info(f"Skipping table: {table_name}")
                continue
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            logging.info(f"Dropped table: {table_name}")

        # Re-enable foreign key checks
        cur.execute("SET session_replication_role = DEFAULT;")

        conn.commit()

    except Exception as e:
        logging.error(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# History data fill
def create_and_fill_table(df, database_config):
    
    try:
        # Get table name from the first row's symbol in the DataFrame
        table_name = df["Symbol"].iloc[0]
        logging.info(f"Filling database table: {table_name}")
        # Convert DataFrame to list of tuples
        data = [
            (
                row["Symbol"],
                row["Date"],
                row["Time"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"],
                row["VWAP"],
                row["EMA9"],
                row["Avg_volume"],
                row["Rvol"],
                row["Relatr"]
            )
            for _, row in df.iterrows()
        ]

        # Build SQL with capitalized column names
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                "Symbol" TEXT NOT NULL,
                "Date" DATE NOT NULL,
                "Time" TIME NOT NULL,
                "Open" NUMERIC(10, 2),
                "High" NUMERIC(10, 2),
                "Low" NUMERIC(10, 2),
                "Close" NUMERIC(10, 2),
                "Volume" NUMERIC(18, 2),
                "VWAP" NUMERIC(10, 2),
                "EMA9" NUMERIC(10, 2),
                "Avg_volume" NUMERIC(18, 2),
                "Rvol" NUMERIC(10, 2),
                "Relatr" NUMERIC(10, 2)

            );
        """

        insert_sql = f"""
        INSERT INTO {table_name} 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Avg_volume", "Rvol", "Relatr")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        conn, cur = get_connection_and_cursor(database_config)

        # Create table if not exists
        cur.execute(create_table_sql)

        # Insert multiple rows
        cur.executemany(insert_sql, data)

        conn.commit()
       # print(f"Table '{table_name}' created and {len(data)} rows inserted successfully.")

    except Exception as e:
        logging.error(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def create_and_fill_avg_volume_tables(df_list, database_config):
    """
    Create and fill average volume tables for multiple symbols.

    Parameters
    ----------
    df_list : list[pd.DataFrame]
        Each DataFrame must contain ['Symbol', 'Time', 'Avg_volume'].
    database_config : dict
        Database connection info for get_connection_and_cursor().
    """
    conn, cur = None, None
    try:
        conn, cur = get_connection_and_cursor(database_config)

        for df in df_list:
            if df.empty:
                continue

            # Table name = lowercase symbol name
            table_name = df["Symbol"].iloc[0].lower()
            logging.info(f"Filling average volume table: {table_name}")

            # Prepare data
            data = [
                (row["Symbol"], row["Time"], row["Avg_volume"])
                for _, row in df.iterrows()
            ]

            # Create table if not exists
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    "Symbol" TEXT NOT NULL,
                    "Time" TIME NOT NULL,
                    "Avg_volume" NUMERIC(18, 2)
                );
            """

            # Insert statement
            insert_sql = f"""
                INSERT INTO {table_name} ("Symbol", "Time", "Avg_volume")
                VALUES (%s, %s, %s);
            """

            # Execute SQL
            cur.execute(create_table_sql)
            cur.executemany(insert_sql, data)
            conn.commit()

            logging.info(f" Inserted {len(data)} rows into '{table_name}'")

    except Exception as e:
        logging.error(f" Error inserting avg volumes: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

async def get_async_connection(database_config:dict)-> asyncpg.Connection:
    """
    Create and return an async database connection.

    Parameters
    ----------
    database_config : dict
        Dictionary with keys: user, password, database, host, port (optional)

    Returns
    -------
    asyncpg.Connection
    """
    try:
        conn = await asyncpg.connect(
            user=database_config["user"],
            password=database_config["password"],
            database=database_config["database"],
            host=database_config["host"],
            port=int(database_config.get("port", 5432))
        )
        return conn
    except Exception as e:
        logging.exception("Failed to create async database connection: %s", e)
        raise


async def insert_candlestick_row(last_candle: CandleRow, database_config: dict):
    """
    Async: Save a single candlestick row to the database if it doesn't already exist.
    Uses CandleRow dataclass for type safety and Decimal precision.
    """

    symbol = last_candle.symbol.lower()
    conn = await get_async_connection(database_config)

    try:
        # Convert CandleRow → tuple for DB insertion
        db_row = (
            last_candle.symbol,
            last_candle.date,
            last_candle.time,
            last_candle.open,
            last_candle.high,
            last_candle.low,
            last_candle.close,
            last_candle.volume,
            last_candle.vwap,
            last_candle.ema9,
            last_candle.avg_volume,
            last_candle.rvol,
            last_candle.relatR,
        )

        # --- Check if record already exists ---
        check_sql = f"""
        SELECT 1 FROM "{symbol}"
        WHERE "Symbol"=$1 AND "Date"=$2 AND "Time"=$3
        LIMIT 1;
        """
        exists = await conn.fetchrow(check_sql, last_candle.symbol, last_candle.date, last_candle.time)
        if exists:
            logging.info(f"Skipped duplicate candle for {last_candle.symbol}: {last_candle}")
            return

        # --- Insert new record ---
        insert_sql = f"""
        INSERT INTO "{symbol}" 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9","Avg_volume", "Rvol","Relatr")
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);
        """
        await conn.execute(insert_sql, *db_row)
        logging.info(f"Inserted candle into '{symbol}': {last_candle}")

    except Exception as e:
        logging.exception(f" Error inserting row for {last_candle.symbol}: {e}")

    finally:
        if conn:
            await conn.close()


async def insert_bulk_livestream(bars: list[dict], database_config: dict):
    """
    Bulk insert a list of 5-second RealTimeBars into the 'livedata' table.
    Each item in `bars` must contain:
        symbol, time (datetime), last (float), volume (float)
    """
    if not bars:
        return

    try:
        conn = await get_async_connection(database_config)

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS livedata (
            "Symbol" TEXT NOT NULL,
            "Date" DATE NOT NULL,
            "Time" TIMESTAMP WITH TIME ZONE NOT NULL,
            "Last" DOUBLE PRECISION,
            "Volume" DOUBLE PRECISION
        );
        """
        await conn.execute(create_table_query)

        # Bulk insert
        insert_query = """
            INSERT INTO livedata ("Symbol", "Date", "Time", "Last", "Volume")
            VALUES ($1, $2, $3, $4, $5)
        """

        values = []
        for b in bars:
            bar_time = b["time"]
            if not isinstance(bar_time, datetime):
                raise ValueError(f"Invalid 'time' field: {bar_time}")

            values.append((
                b["symbol"],
                b["time"].date(),   # still fine
                b["time"],          # timezone-aware datetime
                b["last"],
                b["volume"]
            ))

        await conn.executemany(insert_query, values)
        await conn.close()

        logging.info(f"Inserted {len(bars)} bars into livedata table.")

    except Exception as e:
        logging.exception("Error inserting bars into livedata table: %s", e)


async def fetch_avg_volume_for_candle(candle_row: CandleRow, database_avgvolume_config) -> float:
    """
    Fetch the average volume for a given candle's symbol and time from the avg volume table.

    Parameters
    ----------
    candle_row : CandleRow
        Dataclass instance containing at least .symbol and .time.
    database_avgvolume_config : dict
        Database configuration to connect to the average volume DB.

    Returns
    -------
    float
        The Avg_volume value for that candle, or 0.0 if not found.
    """
    conn = None
    try:
        symbol = candle_row.symbol
        time_val = candle_row.time
        table_name = symbol.lower()

        conn = await get_async_connection(database_avgvolume_config)

        query = f"""
            SELECT "Avg_volume"
            FROM "{table_name}"
            WHERE "Time" = $1
            LIMIT 1;
        """
        row = await conn.fetchrow(query, time_val)

        # Return 0.0 if row or column is missing, else convert to float
        return float(row["Avg_volume"]) if row and row.get("Avg_volume") is not None else 0.0

    except Exception as e:
        logging.error(f"Error fetching avg volume for {symbol} at {time_val}: {e}")
        return 0.0

    finally:
        if conn:
            await conn.close()


#-----------------Alarms handling----------------------------------------------------------------


async def get_last_rows(table_name:str, num_rows=None, database_config=None):
    """
    Fetch the last `num_rows` from the given table asynchronously.
    If num_rows is None, fetch all available rows.
    Returns a pandas DataFrame.
    """
    conn = None
    try:
        conn = await get_async_connection(database_config)

        if num_rows is None:
            # Fetch all rows
            query = f"""
                SELECT * 
                FROM "{table_name}"
                ORDER BY "Date" ASC, "Time" ASC;
            """
            rows = await conn.fetch(query)
        else:
            # Fetch last num_rows in descending order, then reorder ascending
            query = f"""
                SELECT * FROM (
                    SELECT * FROM "{table_name}"
                    ORDER BY "Date" DESC, "Time" DESC
                    LIMIT $1
                ) sub
                ORDER BY "Date" ASC, "Time" ASC;
            """
            rows = await conn.fetch(query, num_rows)

        if not rows:
            return pd.DataFrame()

        # Convert asyncpg records to DataFrame
        df = pd.DataFrame([dict(r) for r in rows])
                # 🔧 Convert numeric columns (which come as Decimal) to float
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    except Exception as e:
        logging.error(f"Error fetching last rows for {table_name}: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            await conn.close()


async def insert_alarm(candle: CandleRow, alarm_message, database_config):
    """Async insert of an alarm into the database."""
    conn = None
    try:
        conn = await get_async_connection(database_config)

        insert_query = """
            INSERT INTO alarms ("Symbol", "Time", "Alarm", "Date")
            VALUES ($1, $2, $3, $4);
        """
        await conn.execute(insert_query,
                            candle.symbol,
                            candle.time,
                            alarm_message,  # <- third argument: Alarm (string)
                            candle.date     # <- fourth argument: Date (date)
                        )
        logging.info("Alarm inserted: %s %s %s", candle.symbol, candle.time, alarm_message)

    except Exception as e:
        logging.error("Error inserting alarm: %s", e)
    finally:
        if conn:
            await conn.close()


async def alarm_exists_recently(candle:CandleRow, database_config, cutoff_minutes)-> bool:
    """
    Async check if an alarm exists for the symbol within the last `cutoff_minutes`.
    Returns True if exists, False otherwise.
    """
    conn = None
    try:
        conn = await get_async_connection(database_config)

        current_dt = datetime.combine(candle.date, candle.time)
        cutoff_dt = current_dt - timedelta(minutes=cutoff_minutes)

        query = """
            SELECT 1
            FROM alarms
            WHERE "Symbol" = $1
              AND ("Date" > $2
                   OR ("Date" = $3 AND "Time" >= $4))
            LIMIT 1;
        """
        row = await conn.fetchrow(query, candle.symbol, cutoff_dt.date(), cutoff_dt.date(), cutoff_dt.time())
        return row is not None

    except Exception as e:
        logging.error("Error checking alarm existence: %s", e)
        return False
    finally:
        if conn:
            await conn.close()