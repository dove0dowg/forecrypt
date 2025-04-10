# libs
import logging
import subprocess
import json
from typing import Any
from clickhouse_driver import Client
# logger
logger = logging.getLogger(__name__)

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

def create_external_pg_table(clickhouse_client: Client, clickhouse_config: dict[str, Any], postgres_config: dict[str, Any]) -> bool:
    """
    Creates a table in ClickHouse connected to a PostgreSQL database using the provided config.
    
    This function creates a table that links PostgreSQL data to ClickHouse using the PostgreSQL engine 
    and the provided connection details in the config dictionary.
    
    :param config: Dictionary containing PostgreSQL connection details.
    :param clickhouse_client: An instance of the ClickHouse Client for executing the query.
    :return: True if the table was created successfully, False otherwise.
    """
    
    # Assemble the PostgreSQL connection string components
    pg_port = f"{postgres_config['port']}"
    pg_database = postgres_config['dbname']
    pg_user = postgres_config['user']
    pg_password = postgres_config['password']
    pg_container = postgres_config['container_name']
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
        h_uploaded_at DateTime,
        f_uploaded_at DateTime,
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

def create_ch_forecast_data_table(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    """
    Creates a local ClickHouse table 'forecast_data' using ReplacingMergeTree,
    based on data from external_pg_forecast_data.

    The table is designed to accept periodic inserts from the external PostgreSQL engine table,
    using 'uploaded_at' as the version field for deduplication and replacement.

    Args:
        clickhouse_client (Client): Active ClickHouse client.
        clickhouse_config (dict[str, Any]): ClickHouse connection config (used for database/table name).

    Returns:
        bool: True if the table was created successfully, False otherwise.
    """
    ch_database = clickhouse_config['database']
    local_table_name = "forecast_data"

    query = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{local_table_name} (
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
        h_uploaded_at DateTime,
        f_uploaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(h_uploaded_at)
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (timestamp, currency, model, forecast_step);
    """

    try:
        clickhouse_client.execute(query)
        logger.info(f"Local ClickHouse table '{local_table_name}' created successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to create local forecast_data table: {e}")
        return False

def insert_from_external(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    ch_database = clickhouse_config['database']

    query = f"""
    INSERT INTO {ch_database}.forecast_data
    SELECT
        id,
        timestamp,
        currency,
        forecast_step,
        forecast_value,
        historical_value,
        model,
        model_name_ext,
        external_model_params,
        inner_model_params,
        h_uploaded_at, 
        f_uploaded_at
    FROM {ch_database}.external_pg_forecast_data
    """

    try:
        clickhouse_client.execute(query)
        logger.info("Bulk inserted data from external_pg_forecast_data into forecast_data.")
        return True
    except Exception as e:
        logger.error(f"Failed to insert data: {e}")
        return False