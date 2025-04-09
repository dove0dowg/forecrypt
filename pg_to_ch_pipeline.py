# libs
import logging
import subprocess
import json
from typing import Any
from clickhouse_driver import Client
# modules
from config import PG_DB_CONFIG, CH_DB_CONFIG
from db_utils_postgres import update_pg_config, postgres_connection
from db_utils_clickhouse import update_ch_config, clickhouse_connection
# logger
logger = logging.getLogger(__name__)
# ---------------------------------------------------------
# [Transfer data pipeline] (from Postgres to Clickhouse)
# ---------------------------------------------------------
def get_unique_models(postgres_client):
    """
    Fetch unique model names from the combined_data view in PostgreSQL.
    """
    try:
        with postgres_client.cursor() as cursor:
            cursor.execute("SELECT DISTINCT model_name_ext FROM combined_data;")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error in get_unique_models: {e}")
        return []

def get_last_timestamp_from_ch(clickhouse_client, model_name_ext):
    """
    Fetch the latest timestamp for a specific model in ClickHouse.
    """
    try:
        query = f"SELECT MAX(timestamp) FROM combined_data WHERE model_name_ext = '{model_name_ext}';"
        result = clickhouse_client.execute(query)
        return result[0][0] if result[0][0] else '1970-01-01 00:00:00'
    except Exception as e:
        logger.error(f"Error in get_last_timestamp_from_ch: {e}")
        return '1970-01-01 00:00:00'

def get_last_timestamp_from_pg(postgres_client, model_name_ext):
    """
    Fetch the latest timestamp for a specific model in PostgreSQL.
    """
    try:
        with postgres_client.cursor() as cursor:
            cursor.execute("""
                SELECT MAX(timestamp) FROM combined_data WHERE model_name_ext = %s;
            """, (model_name_ext,))
            result = cursor.fetchone()
            return result[0] if result[0] else '1970-01-01 00:00:00'
    except Exception as e:
        logger.error(f"Error in get_last_timestamp_from_pg: {e}")
        return '1970-01-01 00:00:00'

def get_new_data_from_pg(postgres_client, model_name_ext, last_timestamp):
    """
    Fetch new data for a specific model from PostgreSQL where timestamp > last_timestamp.
    """
    try:
        with postgres_client.cursor() as cursor:
            cursor.execute("""
                SELECT id, timestamp, currency, model, model_name_ext, created_at, forecast_step, forecast_value
                FROM combined_data
                WHERE model_name_ext = %s AND timestamp > %s;
            """, (model_name_ext, last_timestamp))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error in get_new_data_from_pg: {e}")
        return []

def insert_data_into_ch(clickhouse_client, model_name_ext, data):
    """
    Insert new data for a specific model into ClickHouse.
    """
    try:
        if data:
            insert_query = f"""
                INSERT INTO {model_name_ext} (id, timestamp, currency, model, model_name_ext, created_at, forecast_step, forecast_value)
                VALUES
            """
            # Prepare values for insertion
            insert_values = ', '.join([f"('{d[0]}', '{d[1]}', '{d[2]}', '{d[3]}', '{d[4]}', '{d[5]}', {d[6]}, {d[7]})" for d in data])
            clickhouse_client.execute(insert_query + insert_values)
            logger.info(f"Data for model {model_name_ext} loaded into ClickHouse.")
        else:
            logger.info(f"No new data for model {model_name_ext}.")
    except Exception as e:
        logger.error(f"Error in insert_data_into_ch: {e}")

def pg_to_ch_pipeline(postgres_client, clickhouse_client) -> bool:
    """
    Main function to transfer data from PostgreSQL to ClickHouse for each model.
    """
    try:
        # Fetch unique models from PostgreSQL
        models = get_unique_models(postgres_client)

        # Process each model
        for model_name_ext in models:
            logger.info(f"Processing model: {model_name_ext}")

            # Fetch the latest timestamps from both databases
            last_timestamp_ch = get_last_timestamp_from_ch(clickhouse_client, model_name_ext)
            last_timestamp_pg = get_last_timestamp_from_pg(postgres_client, model_name_ext)

            # Compare timestamps and choose the latest
            last_timestamp = max(last_timestamp_ch, last_timestamp_pg)

            # Fetch new data from PostgreSQL
            new_data = get_new_data_from_pg(postgres_client, model_name_ext, last_timestamp)

            # Insert the new data into ClickHouse
            insert_data_into_ch(clickhouse_client, model_name_ext, new_data)

            return True
        
    except Exception as e:
        logger.error(f"Error in pg_to_ch_pipeline: {e}")
        
        return False
# ---------------------------------------------------------
# [Create external Postgres table]
# ---------------------------------------------------------

def get_container_ip(container_name: str) -> str:
    try:
        result = subprocess.run(
            f"wsl docker inspect {container_name}",
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        info = json.loads(result.stdout)
        ip = info[0]['NetworkSettings']['IPAddress']
        return ip
    except Exception as e:
        raise RuntimeError(f"Failed to get IP of container '{container_name}': {e}")

def create_external_pg_table(clickhouse_client: Client, clickhouse_config: dict[str, Any], pg_config: dict[str, Any]) -> bool:
    """
    Creates a table in ClickHouse connected to a PostgreSQL database using the provided config.
    
    This function creates a table that links PostgreSQL data to ClickHouse using the PostgreSQL engine 
    and the provided connection details in the config dictionary.
    
    :param config: Dictionary containing PostgreSQL connection details.
    :param clickhouse_client: An instance of the ClickHouse Client for executing the query.
    :return: True if the table was created successfully, False otherwise.
    """
    
    # Assemble the PostgreSQL connection string components
    pg_port = f"{pg_config['port']}"
    pg_database = pg_config['dbname']
    pg_user = pg_config['user']
    pg_password = pg_config['password']
    pg_container = pg_config['container_name']
    pg_container_ip = get_container_ip(pg_container)
    
    #win_ip = get_windows_host_ip(clickhouse_config)
    ch_database_name = clickhouse_config['database']
    ch_table_name = clickhouse_config['table']
    
    # Prepare the query to create the table in ClickHouse
    query = f"""
    CREATE TABLE {ch_database_name}.{ch_table_name}
    (
        id String,
        timestamp DateTime,
        currency String,
        forecast_step Int32,
        forecast_value Float64,
        historical_value Float64,
        model String,
        model_name_ext String,
        external_model_params String,
        inner_model_params String,
        uploaded_at DateTime,
        config_start DateTime,
        config_end DateTime
    ) 
    ENGINE = PostgreSQL('{pg_container_ip}:5432', '{pg_database}', 'backtest_data_mv', '{pg_user}', '{pg_password}');
    """
    logger.debug(query)
    # Execute the query via the provided ClickHouse client
    try:
        clickhouse_client.execute(query)  # Use the provided client to execute the query
        logger.info(f"Table {ch_table_name} created successfully.")
        return True  # Success
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return False  # Failure

