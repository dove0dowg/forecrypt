import psycopg2
import pandas as pd
from uuid import uuid4
from config import DB_CONFIG
from datetime import datetime, timezone, timedelta
from psycopg2.extras import execute_values

def create_tables():
    """
    create tables historical_data и forecast_data, if there are none in database
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_data (
                    id UUID PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    currency VARCHAR(50) NOT NULL,
                    price DECIMAL(18, 8) NOT NULL,
                    UNIQUE (timestamp, currency)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS forecast_data (
                    id UUID PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    currency VARCHAR(50) NOT NULL,
                    model VARCHAR(100) NOT NULL,
                    forecast_step INTEGER NOT NULL,
                    forecast_value DECIMAL(18, 8) NOT NULL,
                    UNIQUE (timestamp, currency, model, forecast_step)
                );
            """)
            conn.commit()

def get_missing_hours(crypto_id: str, start_date, end_date) -> list:
    """
    Get a list of missing hourly timestamps for a given cryptocurrency within a date range.
    """

    query = """
        WITH full_range AS (
            SELECT generate_series(%s::timestamptz, %s::timestamptz, '1 hour') AS ts
        )
        SELECT ts
        FROM full_range
        WHERE ts NOT IN (
            SELECT timestamp AT TIME ZONE 'UTC'
            FROM historical_data
            WHERE currency = %s
        )
        ORDER BY ts;
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC';")  # Установить UTC
            cursor.execute(query, (start_date, end_date, crypto_id))
            missing_hours = [row[0] for row in cursor.fetchall()]

    return missing_hours

def check_consistency(crypto_id: str, start_date: str) -> bool:
    """
    сonsistency check for no spaces between timestamps
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT timestamp FROM historical_data
                WHERE currency = %s AND timestamp >= %s
                ORDER BY timestamp;
            """
            cursor.execute(query, (crypto_id, start_date))
            timestamps = [row[0] for row in cursor.fetchall()]

            for i in range(1, len(timestamps)):
                if timestamps[i] - timestamps[i - 1] != timedelta(hours=1):
                    return False

            return True

def load_to_db_historical(dataframe, crypto_id, conn):
    """
    save historical data into the database table `historical_data`
    """
    if dataframe.empty:
        print(f"No data to load for {crypto_id}.")
        return

    dataframe['price'] = dataframe['price'].round(8)

    records = [
        (str(uuid4()), row['date'], crypto_id, row['price'])
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO historical_data (id, timestamp, currency, price)
        VALUES %s
        ON CONFLICT (timestamp, currency)
        DO NOTHING;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            print(f"Data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        print(f"Failed to load data for {crypto_id}. Error: {e}")
