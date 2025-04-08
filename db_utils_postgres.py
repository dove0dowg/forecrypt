import psycopg2
import pandas as pd
import logging
import os
import subprocess
import time
from uuid import uuid4
from config import PG_DB_CONFIG
from datetime import datetime, timezone, timedelta
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from typing import Any

load_dotenv()

logger = logging.getLogger("forecrypt")
# ---------------------------------------------------------
# [Postgres container]
# ---------------------------------------------------------

def ensure_docker_running() -> bool:
    """
    Ensures the Docker daemon is running in WSL.

    This function checks if the Docker daemon is active by running `docker ps`.
    If the daemon is not running, it attempts to start it using `service docker start`.
    First, it tries starting without root privileges, then retries with `wsl -u root`
    if the initial attempt fails.

    Returns:
        bool: True if Docker is running or successfully started, False otherwise.
    """
    try:
        check_cmd = "wsl docker ps"
        result = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            logger.info("Docker daemon is already running")
            return True
            
        logger.warning("Docker daemon not running, attempting to start...")
        
        start_cmd = "wsl service docker start"
        result = subprocess.run(start_cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            start_cmd = "wsl -u root service docker start"
            result = subprocess.run(start_cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            logger.info("Docker daemon started successfully")
            return True
            
        logger.error(f"Failed to start Docker: {result.stderr}")
        return False
        
    except Exception as e:
        logger.exception(f"Error starting Docker: {str(e)}")
        return False

def remove_postgres_container(container_name: str):
    """
    Forcefully removes a PostgreSQL Docker container if it exists.

    This function attempts to remove the specified PostgreSQL container using `docker rm -f`.
    If the container does not exist, it logs a debug message instead of raising an error.

    Args:
        container_name (str): The name of the PostgreSQL container to remove.
    """
    try:
        logger.debug(f"Attempting to remove container: {container_name}")
        result = subprocess.run(f"wsl docker rm -f {container_name}", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Removed existing container: {container_name}")
        else:
            logger.debug(f"No container to remove: {container_name}")
    except Exception as e:
        logger.warning(f"Error removing container: {str(e)}")

def start_postgres_with_volume(pg_config: dict[str, Any]) -> bool:
    """
    Creates a named Docker volume (pg_data) and runs a PostgreSQL container
    with the specified config.

    Args:
        pg_config (dict): A dictionary containing:
            - 'container_name' (str): Desired name for the container.
            - 'port' (int): Host port to expose PostgreSQL on (maps to 5432 in container).
            - 'user' (str, optional): PostgreSQL user. Defaults to 'postgres' if not set.
            - 'password' (str, optional): PostgreSQL password. Defaults to 'secret' if not set.

    Returns:
        bool: True if the container starts successfully, False otherwise.
    """
    try:
        volume_name = "pg_data"
        container_name = pg_config.get('container_name', 'postgres_container')
        port = pg_config.get('port', 5432)
        user = pg_config.get('user') or 'postgres'
        password = pg_config.get('password') or 'secret'

        # 1) Create named volume
        create_vol_cmd = f"wsl docker volume create {volume_name}"
        logger.info(f"Creating named volume with command: {create_vol_cmd}")
        subprocess.run(create_vol_cmd, shell=True, check=True)

        # 2) Run PostgreSQL container with this volume
        run_cmd = (
            "wsl docker run -d "
            f"--name {container_name} "
            f"-p {port}:5432 "
            f"-e POSTGRES_USER={user} "
            f"-e POSTGRES_PASSWORD={password} "
            f"-v {volume_name}:/var/lib/postgresql/data "
            "postgres"
        )

        logger.info(f"Starting PostgreSQL container with command: {run_cmd}")
        result = subprocess.run(
            run_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode == 0:
            logger.info(f"Container '{container_name}' started successfully. Volume: {volume_name}")
            logger.debug(f"docker run output: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"Failed to start container. Exit code: {result.returncode}")
            logger.error(f"Error output: {result.stderr.strip()}")
            return False

    except subprocess.CalledProcessError as e:
        logger.error(f"Error creating volume or running container: {e.stderr}")
        return False
    except subprocess.TimeoutExpired:
        logger.error("Command timed out after 300 seconds.")
        return False
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return False

def client_check_postgres_ready(pg_config: dict[str, str]) -> bool:
    """
    Repeatedly checks if PostgreSQL is ready to accept connections via psycopg2.

    This function attempts to establish a connection and execute a simple SELECT 1 query
    up to 20 times with a 0.5-second interval, ensuring PostgreSQL is operational.

    Args:
        pg_config (dict[str, str]): A dictionary containing PostgreSQL connection parameters.

    Returns:
        bool: True if PostgreSQL responds successfully within the attempts, False otherwise.

    Logs:
        - INFO: Each attempt result (success or failure).
        - DEBUG: Detailed error messages if a failure occurs.
        - ERROR: If all attempts fail.
    """
    for attempt in range(1, 21):  # 20 attempts
        try:
            conn = psycopg2.connect(
                host=pg_config["host"],
                port=pg_config["port"],
                user=pg_config["user"],
                password=pg_config["password"],
                dbname=pg_config.get("dbname", "postgres")
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result == (1,):
                    logger.info(f"PostgreSQL is ready and accepting queries (attempt {attempt}/20).")
                    conn.close()
                    return True
                else:
                    logger.info(f"PostgreSQL responded unexpectedly (attempt {attempt}/20): {result}")
            conn.close()

        except Exception as e:
            logger.info(f"PostgreSQL is not ready yet (attempt {attempt}/20).")
            logger.debug(f"Exception details: {e}")

        time.sleep(0.5)

    logger.error("PostgreSQL did not become ready after 20 attempts.")
    return False

# final [Postgres container] function
def postgres_container_forced_install(postgres_config: dict[str, Any]):
    """
    Forces the installation and setup of the PostgreSQL container.

    This function performs the following steps:
    1. Verifies if Docker is running by invoking the `ensure_docker_running()` function.
    2. Removes any pre-existing PostgreSQL container using the `remove_postgres_container()` function.
    3. Initializes volumes and configuration files by calling the `init_postgres_volumes_and_config()` function.
    4. Attempts to start the PostgreSQL container with the `initial_start_postgres_container()` function.

    Returns:
        bool: `True` if PostgreSQL container was successfully installed and started, `False` otherwise.
    """
    try:
        logger.info("=== Starting PostgreSQL Install and Deploy ===")
        
        if not ensure_docker_running():
            logger.error("Docker is not running! Aborting.")
            return False    
        
        remove_postgres_container(postgres_config["container_name"])
        
        if not start_postgres_with_volume(postgres_config):
            logger.error("Couldn't start PostgreSQL docker container! Aborting.")
            return False         

        if not client_check_postgres_ready(postgres_config):
            logger.error("PostgreSQL doesn't answer requests! Aborting.")
            return False   

        return True
    
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return False
# ---------------------------------------------------------
# [Init Postgres and create tables]
# ---------------------------------------------------------
def update_pg_config(config: dict [str, Any]) -> dict [str, Any]:
    """Update Postgres config from ernvironment, if present"""
    active_config = config.copy()
    active_config.update({
        'dbname': os.getenv('FORECRYPT_PG_DB_NAME', active_config['dbname']),
        'user': os.getenv('FORECRYPT_PG_DB_USER', active_config['user']),
        'password': os.getenv('FORECRYPT_PG_DB_PASS', active_config['password']),
        'host': os.getenv('FORECRYPT_PG_DB_HOST', active_config['host']),
        'port': int(os.getenv('FORECRYPT_PG_DB_PORT', active_config['port'])),
        'container_name': os.getenv('FORECRYPT_PG_DB_CONTAINER', active_config['container_name']),
    })
    return active_config

def postgres_connection(**kwargs):
    """
    Initialize a psycopg2 connection using only valid parameters.

    :param kwargs: Connection parameters (must include dbname, user, etc.).
    :return: psycopg2 connection object.
    """
    try:
        # only keep valid psycopg2 params
        valid_keys = {'dbname', 'user', 'password', 'host', 'port'}
        conn_params = {k: v for k, v in kwargs.items() if k in valid_keys}

        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        logger.info("database connection established.")
        return conn

    except psycopg2.Error as e:
        logger.error(f"failed to connect to the database: {e}")
        exit()

def create_database(config: dict):
    """
    Creates a PostgreSQL database from config['dbname'], if it does not exist.
    Uses a direct connection with autocommit mode (CREATE DATABASE not allowed in transactions).

    :param config: Dictionary with at least 'dbname', 'user', 'password', 'host', 'port'.
    """
    import psycopg2
    from psycopg2 import sql

    dbname = config['dbname']
    base_conn_params = {k: v for k, v in config.items() if k in {'user', 'password', 'host', 'port'}}

    try:
        conn = psycopg2.connect(dbname="postgres", **base_conn_params)
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            logger.info(f"database '{dbname}' created.")
        else:
            logger.info(f"database '{dbname}' already exists.")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        logger.error(f"failed to create database '{dbname}': {e}")
        exit()

def create_tables(conn):
    """
    Create tables historical_data и forecast_data, if there are none in database
    """
    with conn.cursor() as cursor:
        # historical_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_data (
                id UUID PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                currency VARCHAR(50) NOT NULL,
                value DECIMAL(18, 8) NOT NULL,
                data_label VARCHAR(20) NOT NULL CHECK (data_label IN ('historical', 'training')),
                uploaded_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (timestamp, currency, data_label)
            );
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_historical_data_timestamp_currency ON historical_data (timestamp, currency);
        """)
        # forecast_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecast_data (
                id UUID PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                currency VARCHAR(50) NOT NULL,
                forecast_step INTEGER NOT NULL,
                forecast_value DECIMAL(18, 8) NOT NULL,
                model VARCHAR(100) NOT NULL,
                model_name_ext VARCHAR(100) NOT NULL,
                external_model_params VARCHAR(100) NOT NULL,
                inner_model_params VARCHAR(100) NOT NULL, 
                zero_step_ts TIMESTAMP NOT NULL,
                config_start TIMESTAMP NOT NULL,
                config_end TIMESTAMP NOT NULL,
                uploaded_at TIMESTAMP DEFAULT NOW(), 
                UNIQUE (timestamp, currency, model, forecast_step)
            );
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp ON forecast_data (timestamp, currency);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp_currency_model ON forecast_data (timestamp, currency, model);
        """)
        logger.info("database tables initialized.")
        
        conn.commit()

def create_materialized_view(conn):
    """
    Create a materialized view for combining historical and forecast data in PostgreSQL.
    
    :param conn: Active connection to PostgreSQL.
    """
    query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS backtest_data_mv AS
    SELECT 
        f.id,
        f.timestamp,
        f.currency,
        f.forecast_step,
        f.forecast_value,
        h.value AS historical_value,
        f.model,
        f.model_name_ext,
        f.external_model_params,
        f.inner_model_params,
        f.uploaded_at,
        f.zero_step_ts,
        f.config_start,
        f.config_end
    FROM forecast_data f
    LEFT JOIN historical_data h 
    ON f.timestamp = h.timestamp AND f.currency = h.currency
    WITH NO DATA;
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            logger.info("Materialized view 'backtest_data_mv' created successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create materialized view: {e}")

def refresh_materialized_view(conn):
    """
    Refresh the materialized view backtest_data_mv to include only new data.
    This will recalculate and update the materialized view with the latest data.
    
    :param conn: Active connection to PostgreSQL.
    """
    try:
        with conn.cursor() as cursor:
            # Refresh materialized view
            cursor.execute("""
                REFRESH MATERIALIZED VIEW backtest_data_mv;
            """)
            conn.commit()
            logger.info("Materialized view 'backtest_data_mv' successfully refreshed.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to refresh materialized view: {e}")

def delete_mv_and_tables(conn):
    """
    Delete the view 'combined_data' and the tables 'historical_data' and 'forecast_data' from the database. [DEVELOPMENT MODE]
    """
    queries = [
        "DROP MATERIALIZED VIEW IF EXISTS backtest_data_mv;",
        "DROP TABLE IF EXISTS historical_data CASCADE;",
        "DROP TABLE IF EXISTS forecast_data CASCADE;"
    ]

    try:
        with conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
                logger.info(f"Executed: {query}")
        conn.commit()
        logger.info("Materialized view and tables deleted successfully.")
    except Exception as e:
        logger.critical(f"Failed to delete view and tables. Error: {e}")
# final [Init Postgres and create tables] function
def init_postgres_and_create_tables(postgres_client):

    client_check_postgres_ready
    # remove existing tables if present
    delete_mv_and_tables(postgres_client)

    # create database tables
    create_tables(postgres_client)
    # create nodata materialized view
    create_materialized_view(postgres_client)
    
    return postgres_client
# ---------------------------------------------------------
# [Prepare Postgres] (final function to call)

def prepare_postgres() -> bool:
    """
    Full PostgreSQL setup routine.

    Loads environment config, starts the container, creates the target database
    if missing, establishes a connection, and initializes required tables.
    """
    try:
        load_dotenv(override=True)

        # update config from environment
        postgres_config = update_pg_config(PG_DB_CONFIG)

        # start container if not running
        postgres_container_forced_install(postgres_config)

        # create database if missing
        create_database(postgres_config)

        # connect to target database
        postgres_client = postgres_connection(**postgres_config)

        # create required tables and indexes
        init_postgres_and_create_tables(postgres_client)

        return True

    except Exception as e:
        logger.critical(f"Failed to prepare Postgres. Error: {e}")
        return False
# ---------------------------------------------------------
# [Postgres Queries] (selects, loadings, checks)
# ---------------------------------------------------------
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

    with psycopg2.connect(**PG_DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC';")  # UTC
            cursor.execute(query, (start_date, end_date, crypto_id))
            missing_hours = [row[0] for row in cursor.fetchall()]

    return missing_hours

def check_consistency(crypto_id: str, start_date: str) -> bool:
    """
    сonsistency check for no spaces between timestamps
    """
    with psycopg2.connect(**PG_DB_CONFIG) as conn:
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
        INSERT INTO historical_data (id, timestamp, currency, value, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET value = EXCLUDED.value
        WHERE historical_data.value <> EXCLUDED.value;
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
        INSERT INTO historical_data (id, timestamp, currency, value, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET value = EXCLUDED.value
        WHERE historical_data.value <> EXCLUDED.value;
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
        INSERT INTO historical_data (id, timestamp, currency, value, data_label)
        VALUES %s
        ON CONFLICT (timestamp, currency, data_label)
        DO UPDATE 
        SET value = EXCLUDED.value
        WHERE historical_data.value <> EXCLUDED.value;
    """

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, records)
            conn.commit()
            logger.info(f"Training data for {crypto_id} successfully loaded.")
    except Exception as e:
        conn.rollback()
        logger.critical(f"Failed to load training data for {crypto_id}. Error: {e}")

def load_to_db_forecast(dataframe, crypto_id, model_name, params, conn, zero_step_ts, config_start, config_end):
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

    external_model_params = (
    f"[TD={params['training_dataset_size']}]_"
    f"[MU={params['model_update_interval']}]_"
    f"[FD={params['forecast_dataset_size']}]_"
    f"[FF={params['forecast_frequency']}]_"
    f"[FH={params['forecast_hours']}]"
)

    specific_params = params.get('specific_parameters', {})

    inner_model_params = "_".join(f"[{k}={v}]" for k, v in specific_params.items())

    # Round price to 8 decimal places and create forecast steps
    dataframe['price'] = dataframe['price'].round(8)
    dataframe['step'] = range(0, len(dataframe))

    # Prepare records for insertion
    records = [
        (
            str(uuid4()),          # id
            row['date'],           # timestamp
            crypto_id,             # currency
            row['step'],           # forecast_step
            row['price'],          # forecast_value
            model_name,            # model
            dynamic_model_name,    # model_name_ext
            external_model_params, # external_model_params
            inner_model_params,    # inner_model_params
            zero_step_ts,          # zero_step_ts
            config_start,          # config_start
            config_end             # config_end
        )
        for _, row in dataframe.iterrows()
    ]

    query = """
        INSERT INTO forecast_data (id, timestamp, currency, forecast_step, forecast_value, model, model_name_ext, external_model_params, inner_model_params, zero_step_ts, config_start, config_end)
        VALUES %s
        ON CONFLICT (timestamp, currency, model, forecast_step)
        DO UPDATE 
        SET forecast_value = EXCLUDED.forecast_value,
            zero_step_ts = EXCLUDED.zero_step_ts
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

