import psycopg2
import pandas as pd
import logging
import os
from uuid import uuid4
from config import DB_CONFIG
from datetime import datetime, timezone, timedelta
from psycopg2.extras import execute_values
from dotenv import load_dotenv

logger = logging.getLogger("forecrypt")

def init_database_connection(**kwargs):
    """
    Initialize a database connection. Replaces DB_CONFIG from config.py by .env values. 
    DB_CONFIG from config.py is usable, but .env preferred for security reasons.

    :param kwargs: Database connection parameters (overrides defaults in DB_CONFIG).
    :return: A psycopg2 connection object.
    """

    load_dotenv()

    DB_CONFIG.update({
        'dbname': os.getenv('FORECRYPT_DB_NAME', DB_CONFIG['dbname']),
        'user': os.getenv('FORECRYPT_DB_USER', DB_CONFIG['user']),
        'password': os.getenv('FORECRYPT_DB_PASS', DB_CONFIG['password']),
        'host': os.getenv('FORECRYPT_DB_HOST', DB_CONFIG['host']),
        'port': int(os.getenv('FORECRYPT_DB_PORT', DB_CONFIG['port']))
    })

    try:
        # Если не переданы параметры, используем DB_CONFIG
        conn_params = kwargs if kwargs else DB_CONFIG
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        logger.info("database connection established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"failed to connect to the database: {e}")
        exit()

def create_tables(conn):
    """
    Create tables historical_data и forecast_data, if there are none in database
    """
    with conn.cursor() as cursor:
        # Создание таблицы historical_data
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
            CREATE INDEX IF NOT EXISTS idx_historical_data_timestamp_currency ON historical_data (timestamp, currency);
        """)
        # Создание таблицы forecast_data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecast_data (
                id UUID PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                currency VARCHAR(50) NOT NULL,
                model VARCHAR(100) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                forecast_step INTEGER NOT NULL,
                forecast_value DECIMAL(18, 8) NOT NULL,
                UNIQUE (timestamp, currency, model, forecast_step)
            );
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp ON forecast_data (timestamp);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp_currency_model ON forecast_data (timestamp, currency, model);
        """)
        logger.info("database tables initialized.")
        
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
            cursor.execute("SET TIME ZONE 'UTC';")  # UTC
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
        logger.critical(f"No data to load for {crypto_id}.")
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
            logger.info(f"Data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load data for {crypto_id}. Error: {e}")

def load_to_db_forecast(dataframe, crypto_id, model_name, conn, created_at):
    """
    save forecast data into the database table `forecast_data`.
    
    :param dataframe: forecast data as pandas DataFrame with columns ['date', 'price'].
    :param crypto_id: the cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param model_name: the name of the model (e.g., 'arima', 'ets').
    :param conn: active connection to the PostgreSQL database.
    :param created_at: timestamp representing the starting point of the forecast.
    """
    if dataframe.empty:
        logger.critical(f"No forecast data to load for {crypto_id} - {model_name}.")
        return

    dataframe['price'] = dataframe['price'].round(8)
    dataframe['step'] = range(1, len(dataframe) + 1)

    records = [
        (str(uuid4()), row['date'], crypto_id, model_name, row['step'], row['price'], created_at)
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO forecast_data (id, timestamp, currency, model, forecast_step, forecast_value, created_at)
        VALUES %s
        ON CONFLICT (timestamp, currency, model, forecast_step)
        DO NOTHING;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.debug(f"Forecast data for {crypto_id} - {model_name} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load forecast data for {crypto_id} - {model_name}. Error: {e}")