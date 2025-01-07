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
                data_label VARCHAR(20) NOT NULL CHECK (data_label IN ('historical', 'training')),
                UNIQUE (timestamp, currency, data_label)
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
                model_name_ext VARCHAR(100) NOT NULL,
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

def create_combined_view(conn):
    """
    Create or replace the combined view for historical and forecast data.
    """
    query = """
    CREATE OR REPLACE VIEW combined_data AS
    SELECT 
        id, 
        timestamp, 
        currency, 
        data_label AS model,
        price AS value, 
        0 AS forecast_step, 
        MIN(timestamp) OVER (PARTITION BY data_label) AS created_at,  -- Min timestamp grouped by data_label
        data_label AS model_name_ext 
    FROM 
        historical_data
    UNION ALL
    SELECT 
        id, 
        timestamp, 
        currency, 
        model, 
        forecast_value AS value, 
        forecast_step, 
        created_at,
        model_name_ext 
    FROM 
        forecast_data;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            logger.info("Combined view 'combined_data' created or replaced successfully.")
    except Exception as e:
        logger.error(f"Failed to create combined view: {e}")
        conn.rollback()

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

def load_to_db_train_and_historical(extended_df, crypto_id, conn, max_train_dataset_hours):
    """
    save both training and historical data into the database table `historical_data`.

    the first `max_train_dataset_hours` rows are marked as 'training',
    and the remaining rows are marked as 'historical'.

    :param extended_df: pandas DataFrame with columns ['date', 'price'] containing both training and historical data.
    :param crypto_id: the cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param conn: active connection to the PostgreSQL database.
    :param max_train_dataset_hours: number of hours to be marked as 'training'.
    """
    if extended_df.empty:
        logger.critical(f"No data to load for {crypto_id}.")
        return

    extended_df['price'] = extended_df['price'].round(8)

    # split the data into training and historical based on max_train_dataset_hours
    train_df = extended_df.iloc[:max_train_dataset_hours + 1].copy()  # N-hour inclusion by +1
    train_df['data_label'] = 'training'


    historical_df = extended_df.iloc[max_train_dataset_hours + 1:].copy()  # N-hour exclusion
    historical_df['data_label'] = 'historical'

    logger.info(f"Training data range: {train_df['date'].min()} to {train_df['date'].max()}")
    logger.info(f"Historical data range: {historical_df['date'].min()} to {historical_df['date'].max()}")

    # concatenate training and historical data
    combined_df = pd.concat([train_df, historical_df])

    records = [
        (str(uuid4()), row['date'], crypto_id, row['price'], row['data_label'])
        for _, row in combined_df.iterrows()
    ]

    query = """
        INSERT INTO historical_data (id, timestamp, currency, price, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET price = EXCLUDED.price
        WHERE historical_data.price <> EXCLUDED.price;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.info(f"Training and historical data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load data for {crypto_id}. Error: {e}")

def load_to_db_historical(dataframe, crypto_id, conn):
    """
    save historical data into the database table `historical_data`
    with a 'historical' label
    """
    if dataframe.empty:
        logger.critical(f"No data to load for {crypto_id}.")
        return

    dataframe['price'] = dataframe['price'].round(8)

    # prepare records with 'historical' label
    records = [
        (str(uuid4()), row['date'], crypto_id, row['price'], 'historical')
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO historical_data (id, timestamp, currency, price, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET price = EXCLUDED.price
        WHERE historical_data.price <> EXCLUDED.price;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.info(f"Historical data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load data for {crypto_id}. Error: {e}")

def load_to_db_training(dataframe, crypto_id, conn):
    """
    save training data into the database table `historical_data`
    with a 'training' label
    """
    if dataframe.empty:
        logger.critical(f"No training data to load for {crypto_id}.")
        return

    dataframe['price'] = dataframe['price'].round(8)

    # prepare records with 'training' label
    records = [
        (str(uuid4()), row['date'], crypto_id, row['price'], 'training')
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO historical_data (id, timestamp, currency, price, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET price = EXCLUDED.price
        WHERE historical_data.price <> EXCLUDED.price;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.info(f"Training data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load training data for {crypto_id}. Error: {e}")

def load_to_db_forecast(dataframe, crypto_id, model_name, params, conn, created_at):
    """
    Save forecast data into the database table `forecast_data`.

    The model name is dynamically generated from key parameters to avoid conflicts.

    :param dataframe: Forecast data as pandas DataFrame with columns ['date', 'price'].
    :param crypto_id: The cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param model_name: The base name of the model (e.g., 'arima', 'ets').
    :param params: Dictionary containing model parameters, including 'training_dataset_size',
                   'model_update_interval', 'forecast_dataset_size', 'forecast_frequency',
                   and 'forecast_hours'.
    :param conn: Active connection to the PostgreSQL database.
    :param created_at: Timestamp representing the starting point of the forecast.
    """
    if dataframe.empty:
        logger.critical(f"No forecast data to load for {crypto_id} - {model_name}.")
        return

    # Generate dynamic model name
    dynamic_model_name = (
        f"{model_name}_"
        f"TD{params['training_dataset_size']}_"
        f"MU{params['model_update_interval']}_"
        f"FD{params['forecast_dataset_size']}_"
        f"FF{params['forecast_frequency']}_"
        f"FH{params['forecast_hours']}"
    )

    # Round price to 8 decimal places and create forecast steps
    dataframe['price'] = dataframe['price'].round(8)
    dataframe['step'] = range(0, len(dataframe))

    # Prepare records for insertion
    records = [
        (str(uuid4()), row['date'], crypto_id, model_name, dynamic_model_name, row['step'], row['price'], created_at)
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO forecast_data (id, timestamp, currency, model, model_name_ext, forecast_step, forecast_value, created_at)
        VALUES %s
        ON CONFLICT (timestamp, currency, model, forecast_step)
        DO UPDATE 
        SET forecast_value = EXCLUDED.forecast_value,
            created_at = EXCLUDED.created_at
        WHERE forecast_data.forecast_value <> EXCLUDED.forecast_value;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.debug(f"Forecast data for {crypto_id} - {dynamic_model_name} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load forecast data for {crypto_id} - {dynamic_model_name}. Error: {e}")

def delete_view_and_tables(conn):
    """
    Delete the view 'combined_data' and the tables 'historical_data' and 'forecast_data' from the database.
    """
    queries = [
        "DROP VIEW IF EXISTS combined_data CASCADE;",
        "DROP TABLE IF EXISTS historical_data CASCADE;",
        "DROP TABLE IF EXISTS forecast_data CASCADE;"
    ]

    try:
        with conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
                logger.info(f"Executed: {query}")
        conn.commit()
        logger.info("View and tables deleted successfully.")
    except Exception as e:
        logger.critical(f"Failed to delete view and tables. Error: {e}")