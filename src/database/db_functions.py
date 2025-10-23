import psycopg2
import pandas as pd
from decimal import Decimal
from src.common.calculate import *

from datetime import datetime, timedelta
import logging

import asyncpg

logger = logging.getLogger(__name__)  # module-specific logger


def get_connection_and_cursor(database_config):
    """Create and return a database connection and cursor."""
    conn = psycopg2.connect(**database_config)
    if not conn:
        raise Exception("Failed to connect to database.")
    cur = conn.cursor()
    return conn, cur

async def get_async_connection(database_config):
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





def delete_all_tables_db(database_config):
    conn = None
    cur = None
    try:
        # Check database name first
        db_name = database_config.get("database")
        if db_name != "livestreaming":
            logging.info(f"Aborting: database is '{db_name}', not 'livestreaming'.")
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
            if table_name == "alarms":
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
            "Open" NUMERIC,
            "High" NUMERIC,
            "Low" NUMERIC,
            "Close" NUMERIC,
            "Volume" NUMERIC,
            "VWAP" NUMERIC,
            "EMA9" NUMERIC,
            "Relatr" NUMERIC
        );
        """

        insert_sql = f"""
        INSERT INTO {table_name} 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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


async def insert_candlestick_row(candle_row, database_config):
    """
    Async: Save a single candlestick row to the database if it doesn't already exist.
    Prices are converted to Decimal to avoid float precision issues.
    """
    symbol = candle_row[0].lower()
    conn = None

    try:
        # Convert date and time
        date_obj = candle_row[1]
        if isinstance(date_obj, str):
            date_obj = datetime.strptime(date_obj, "%Y-%m-%d").date()

        time_obj = candle_row[2]
        if isinstance(time_obj, str):
            time_obj = datetime.strptime(time_obj, "%H:%M").time()

        # Convert all price fields to Decimal (Open, High, Low, Close, VWAP, EMA9, Relatr)
        price_fields = [Decimal(str(x)) if x is not None else None for x in candle_row[3:11]]
        # Rebuild row for database insertion
        db_row = [candle_row[0], date_obj, time_obj] + price_fields

        # Connect async
        conn = await get_async_connection(database_config)

        # Check if row exists
        check_sql = f"""
        SELECT 1 FROM "{symbol}"
        WHERE "Symbol"=$1 AND "Date"=$2 AND "Time"=$3
        LIMIT 1;
        """
        exists = await conn.fetchrow(check_sql, db_row[0], db_row[1], db_row[2])
        if exists:
            logging.info("Skipped row (already exists) in table '%s': %s", symbol, candle_row)
            return

        # Insert row
        insert_sql = f"""
        INSERT INTO "{symbol}" 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr")
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);
        """
        await conn.execute(insert_sql, *db_row)
        logging.info("Inserted row into table '%s': %s", symbol, candle_row)

    except Exception as e:
        logging.exception("Error inserting row for '%s': %s", candle_row[0], e)

    finally:
        if conn:
            await conn.close()

# Function to fetch historical data for a symbol
# Function to fetch historical data for a symbol
def fetch_historical_data(symbol, database_config):
    """
    Fetch all historical rows for a given symbol from the database
    and return as a DataFrame with proper numeric types.
    """
    symbol_lower = symbol.lower()
    columns = ["Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr"]

    try:
        conn, cur = get_connection_and_cursor(database_config)
        cur.execute(f"""
            SELECT "Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr"
            FROM "{symbol_lower}"
            ORDER BY "Date", "Time";
        """)
        rows = cur.fetchall()

        if rows:
            df = pd.DataFrame(rows, columns=columns)
            # Ensure numeric columns are proper type
            for col in ["Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df = pd.DataFrame(columns=columns)

        return df

    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol_lower}: {e}")
        return pd.DataFrame(columns=columns)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def handle_next_vwap_and_ema9_values(new_row, database_config):
    """
    Prepare a new_row by fetching historical data and calculating VWAP & EMA9.
    """
    try:
        # Fetch historical data using the reusable function
        df = fetch_historical_data(new_row[0], database_config)

        # Calculate VWAP
        new_row = calculate_next_vwap(new_row, df)

        # Calculate EMA9
        new_row = calculate_next_ema9(new_row, df)

        return new_row

    except Exception as e:
        logging.error(f"Error in handle_next_vwap_and_ema9_values for {new_row[0]}: {e}")
        return new_row
    

#-----------------Alarms handling----------------------------------------------------------------


async def get_last_rows(table_name, num_rows=None, database_config=None):
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
        return df

    except Exception as e:
        logging.error(f"Error fetching last rows for {table_name}: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            await conn.close()



async def insert_alarm(symbol, time_obj, alarm_message, date_obj, database_config):
    """Async insert of an alarm into the database."""
    conn = None
    try:
        conn = await get_async_connection(database_config)

        insert_query = """
            INSERT INTO alarms ("Symbol", "Time", "Alarm", "Date")
            VALUES ($1, $2, $3, $4);
        """
        await conn.execute(insert_query, symbol, time_obj, alarm_message, date_obj)
        logging.info("Alarm inserted: %s %s %s", symbol, time_obj, alarm_message)

    except Exception as e:
        logging.error("Error inserting alarm: %s", e)
    finally:
        if conn:
            await conn.close()


async def alarm_exists_recently(symbol, time_obj, date_obj, database_config, cutoff_minutes=15):
    """
    Async check if an alarm exists for the symbol within the last `cutoff_minutes`.
    Returns True if exists, False otherwise.
    """
    conn = None
    try:
        conn = await get_async_connection(database_config)

        current_dt = datetime.combine(date_obj, time_obj)
        cutoff_dt = current_dt - timedelta(minutes=cutoff_minutes)

        query = """
            SELECT 1
            FROM alarms
            WHERE "Symbol" = $1
              AND ("Date" > $2
                   OR ("Date" = $3 AND "Time" >= $4))
            LIMIT 1;
        """
        row = await conn.fetchrow(query, symbol, cutoff_dt.date(), cutoff_dt.date(), cutoff_dt.time())
        return row is not None

    except Exception as e:
        logging.error("Error checking alarm existence: %s", e)
        return False
    finally:
        if conn:
            await conn.close()