import pandas as pd

from src.common.calculate import *
from src.helpers.handle_candles import *

from datetime import datetime, timedelta
import logging

from src.helpers.utils import *
from src.dependencies import get_db_pool


logger = logging.getLogger(__name__)  # module-specific logger



async def delete_all_tables_db_async() -> None:
    try:
        pool = get_db_pool()

        async with pool.acquire() as conn:
            # Optional safety check (still useful)
            db_name = await conn.fetchval("SELECT current_database();")
            if db_name not in ("livestreaming", "volumemodels"):
                logger.info(
                    f"Aborting: database is '{db_name}', not 'livestreaming' or 'volumemodels'."
                )
                return

            # Fetch all table names
            tables = await conn.fetch("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public';
            """)

            if not tables:
                logger.warning("No tables found in the database.")
                return

            # Disable foreign key checks
            await conn.execute("SET session_replication_role = replica;")

            # Drop tables containing _livestream or _volume_model
            await conn.execute("""
                DO $$
                DECLARE
                    t RECORD;
                BEGIN
                    FOR t IN
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                        AND (tablename LIKE '%_livestream%' OR tablename LIKE '%_volume_model%')
                    LOOP
                        EXECUTE format('DROP TABLE IF EXISTS "%I" CASCADE;', t.tablename);
                    END LOOP;
                END $$;
            """)

            # Re-enable foreign key checks
            await conn.execute("SET session_replication_role = DEFAULT;")

    except Exception:
        logger.exception("Error deleting tables")
        raise


async def create_and_fill_table_async(df: pd.DataFrame)-> None:

    table_name = f"{df["Symbol"].iloc[0]}_livestream"

    # Convert DataFrame to list of tuples.
    # Previously used df.iterrows(), which is notoriously slow because it
    # rebuilds a pandas Series for every row. Selecting the desired columns
    # and calling .itertuples(index=False, name=None) yields plain tuples
    # via numpy buffers -- typically 5-10x faster on multi-row frames and
    # produces tuples already in the right column order for the INSERT.
    _cols = ["Symbol", "Date", "Time", "Open", "High", "Low", "Close",
             "Volume", "VWAP", "EMA9", "Avg_volume", "Rvol", "Relatr"]
    data = list(df[_cols].itertuples(index=False, name=None))

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

    # Composite index on (Symbol, Date, Time) to accelerate:
    #   - duplicate-check SELECTs in insert_candlestick_row
    #   - ORDER BY Date, Time in get_last_rows
    # Without this, both fall back to full table scans as the table grows.
    create_index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_sym_dt_tm
        ON {table_name} ("Symbol", "Date", "Time");
    """

    insert_sql = f"""
    INSERT INTO {table_name}
    ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Avg_volume", "Rvol", "Relatr")
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
    """

    conn = None
    try:
        # CHANGED: use global pool instead of direct connection
        pool = get_db_pool()
        conn = await pool.acquire()

        async with conn.transaction():  # ensures rollback on failure
            await conn.execute(create_table_sql)
            await conn.execute(create_index_sql)
            await conn.executemany(insert_sql, data)

        logging.info(
            f"Table '{table_name}' created and {len(data)} rows inserted successfully."
        )

    except Exception as e:
        logging.error(f"Error filling table '{table_name}': {e}")
        raise

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def create_and_fill_avg_volume_tables_async(df_list: list[pd.DataFrame]):

    try:

        pool = get_db_pool()
        conn = await pool.acquire()

        for df in df_list:
            if df is None or df.empty:
                continue

            table_name = f"{df['Symbol'].iloc[0].lower()}_volume_model"
            logging.info(f"Filling average volume table: {table_name}")

            # Convert DataFrame to list of tuples -- vectorized.
            # See note on iterrows() in create_and_fill_table_async above.
            data = list(
                df[["Symbol", "Time", "Avg_volume"]].itertuples(index=False, name=None)
            )

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                "Symbol" TEXT NOT NULL,
                "Time" TIME NOT NULL,
                "Avg_volume" NUMERIC(18, 2)
            );
            """

            # Index on (Symbol, Time) to accelerate fetch_avg_volume_for_candle,
            # which is hit on every incoming candle.
            create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_sym_time
                ON {table_name} ("Symbol", "Time");
            """

            insert_sql = f"""
            INSERT INTO {table_name} ("Symbol", "Time", "Avg_volume")
            VALUES ($1, $2, $3);
            """

            # Use transaction for safety
            async with conn.transaction():
                await conn.execute(create_table_sql)
                await conn.execute(create_index_sql)
                await conn.executemany(insert_sql, data)

            logging.info(f"Inserted {len(data)} rows into '{table_name}'")

    except Exception as e:
        logging.error(f"Error inserting avg volumes: {e}")
        raise

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def insert_candlestick_row(last_candle: CandleRow):
    """
    Async: Save a single candlestick row to the database if it doesn't already exist.
    Uses CandleRow dataclass for type safety and Decimal precision.
    """

    symbol = last_candle.symbol.lower()
    table_name = f"{symbol}_livestream"
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    try:
        # Convert CandleRow -> tuple for DB insertion
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
        SELECT 1 FROM "{table_name}"
        WHERE "Symbol"=$1 AND "Date"=$2 AND "Time"=$3
        LIMIT 1;
        """
        exists = await conn.fetchrow(check_sql, last_candle.symbol, last_candle.date, last_candle.time)
        if exists:
            logging.debug(f"Skipped duplicate candle for {last_candle.symbol}: {last_candle}")
            return

        # --- Insert new record ---
        insert_sql = f"""
        INSERT INTO "{table_name}" 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9","Avg_volume", "Rvol","Relatr")
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);
        """
        await conn.execute(insert_sql, *db_row)
        logging.debug(f"Inserted candle into '{symbol}': {last_candle}")

    except Exception as e:
        logging.exception(f" Error inserting row for {last_candle.symbol}: {e}")

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def insert_bulk_livestream(bars: list[dict]):
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    if not bars:
        return

    try:

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

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def fetch_avg_volume_for_candle(candle_row: CandleRow) -> float:
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()


    try:
        symbol = candle_row.symbol
        time_val = candle_row.time
        table_name = f"{symbol.lower()}_volume_model"

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
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)



#-----------------Alarms handling----------------------------------------------------------------

async def get_last_rows(table_name:str, num_rows:int):

    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    try:

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
                # Convert numeric columns (which come as Decimal) to float
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    except Exception as e:
        logging.error(f"Error fetching last rows for {table_name}: {e}")
        return pd.DataFrame()

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def insert_alarm(candle: CandleRow, alarm_message:str):
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    try:

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
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)

#------------------Order handling--------------------------------------------------------------

async def insert_order(candle: CandleRow, stop_level: float):
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    try:
        # Ensure orders table exists (with id)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS orders (
            "Id" BIGSERIAL PRIMARY KEY,
            "Symbol" TEXT NOT NULL,
            "Time" TIME NOT NULL,
            "Stop" NUMERIC(10, 2) NOT NULL,
            "Date" DATE NOT NULL,
            "Status" TEXT NOT NULL
        );
        """
        await conn.execute(create_table_query)

        insert_query = """
            INSERT INTO orders ("Symbol", "Time", "Stop", "Date", "Status")
            VALUES ($1, $2, $3, $4, $5);
        """

        await conn.execute(
            insert_query,
            candle.symbol,     # Symbol column
            candle.time,       # Time column
            stop_level,         # Stop column
            candle.date,       # Date column
            "active"           # Status column
        )

        logging.info(
            "Order inserted: %s %s Stop: %.2f Status: active",
            candle.symbol, candle.time, stop_level
        )

    except Exception as e:
        logging.error("Error inserting order: %s", e)

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)


async def alarm_exists_recently(candle: CandleRow, alarm_message: str, cutoff_minutes) -> bool:
    # Get database connection from pool
    pool = get_db_pool()
    conn = await pool.acquire()

    try:

        current_dt = datetime.combine(candle.date, candle.time)
        cutoff_dt = current_dt - timedelta(minutes=cutoff_minutes)

        query = """
            SELECT 1
            FROM alarms
            WHERE "Symbol" = $1
              AND "Alarm" = $2
              AND ("Date" > $3
                   OR ("Date" = $4 AND "Time" >= $5))
            LIMIT 1;
        """

        row = await conn.fetchrow(
            query,
            candle.symbol,
            alarm_message,
            cutoff_dt.date(),
            cutoff_dt.date(),
            cutoff_dt.time()
        )

        return row is not None

    except Exception as e:
        logging.error("Error checking alarm existence: %s", e)
        return False

    finally:
        # CHANGED: release connection back to pool
        if conn:
            await pool.release(conn)
