import psycopg2
import pandas as pd

from src.common.calculate import *

from datetime import datetime, timedelta
import logging

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


def save_candlestick_row_to_db(candle_row, database_config):
    """
    Save a single candlestick row to the database if it doesn't already exist.
    Logs the result and any errors; does not return anything.
    """
    conn = cur = None
    try:
        symbol = candle_row[0].lower()  # Table name is the symbol

        # Connect using provided settings
        conn, cur = get_connection_and_cursor(database_config)

        # Check if the row already exists
        check_sql = f"""
        SELECT 1 FROM "{symbol}" 
        WHERE "Symbol"=%s AND "Date"=%s AND "Time"=%s
        LIMIT 1;
        """
        cur.execute(check_sql, (candle_row[0], candle_row[1], candle_row[2]))
        if cur.fetchone():
            logging.info("Skipped row (already exists) in table '%s': %s", symbol, candle_row)
            return

        # Insert the row
        insert_sql = f"""
        INSERT INTO "{symbol}" 
        ("Symbol", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "VWAP", "EMA9", "Relatr")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(insert_sql, candle_row)
        conn.commit()
        logging.info("Inserted row into table '%s': %s", symbol, candle_row)

    except Exception as e:
        logging.exception("Error inserting row for '%s': %s", candle_row[0], e)
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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


# Hakee tietystä taulusta viimeiset rivit ja palauttaa ne pandas DataFrame -muodossa
def get_last_rows(table_name, num_rows, database_config):
    try:
        conn, cursor = get_connection_and_cursor(database_config)

        # Fetch the latest `num_rows` in descending order, then reorder ascending
        select_query = f"""
            SELECT * FROM (
                SELECT * FROM {table_name}
                ORDER BY "Date" DESC, "Time" DESC
                LIMIT %s
            ) AS sub
            ORDER BY "Date" ASC, "Time" ASC;
        """

        cursor.execute(select_query, (num_rows,))
        rows = cursor.fetchall()

        # Get column names from cursor.description
        col_names = [desc[0] for desc in cursor.description]

        # Create DataFrame
        df = pd.DataFrame(rows, columns=col_names)

    except Exception as e:
        logging.error("Error: %s", e)
        df = pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return df



# Kirjoittaa tunnistetun alarmin kantaan
def insert_alarm(symbol, time_obj, alarm_message, date_obj, database_config):
    try:
        conn, cursor = get_connection_and_cursor(database_config)

        # Insert the new alarm (date and time are separate columns)
        insert_query = """
            INSERT INTO alarms ("Symbol", "Time", "Alarm", "Date")
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (symbol, time_obj, alarm_message, date_obj))
        conn.commit()

        logging.info("Alarm inserted: %s %s %s", symbol, time_obj, alarm_message)


    except Exception as e:
        logging.error("Error inserting alarm: %s", e)
    finally:
        cursor.close()
        conn.close()


def alarm_exists_recently(symbol, time_obj, date_obj, database_config, cutoff_minutes=15):
    """
    Check if there is already an alarm for this symbol within the last `cutoff_minutes`.
    
    Returns True if an alarm exists within the cutoff, False otherwise.
    """
    try:
        conn, cursor = get_connection_and_cursor(database_config)

        # Combine incoming date & time to one datetime
        current_dt = datetime.combine(date_obj, time_obj)
        cutoff_dt = current_dt - timedelta(minutes=cutoff_minutes)

        # Query for any alarms after cutoff_dt for this symbol
        cursor.execute(
            """
            SELECT 1
            FROM alarms
            WHERE "Symbol" = %s
              AND ("Date" > %s
                   OR ("Date" = %s AND "Time" >= %s))
            LIMIT 1;
            """,
            (symbol, cutoff_dt.date(), cutoff_dt.date(), cutoff_dt.time())
        )
        exists = cursor.fetchone() is not None

        return exists

    except Exception as e:
        logging.error("Error checking alarm existence: %s", e)
        return False  # If error, default to False (no alarm)
    finally:
        if cursor: cursor.close()
        if conn: conn.close()