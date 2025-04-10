# libs
import logging
import subprocess
import json
from typing import Any
from clickhouse_driver import Client
# modules
from .core_columns_generators import (
    gen_sql_ch_create_external_pg_table,
    gen_sql_ch_create_forecast_data_table,
    gen_sql_ch_insert_from_external
)
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
    
    # Prepare the query to create external Postgres Table in ClickHouse
    sql_ch_create_external_pg_table = gen_sql_ch_create_external_pg_table(
        pg_container_ip,  
        pg_database, 
        pg_user, 
        pg_password, 
        ch_database_name, 
        ch_table_name
        )

    logger.debug(sql_ch_create_external_pg_table)
    # Execute the query via the provided ClickHouse client
    try:
        clickhouse_client.execute(sql_ch_create_external_pg_table)  # Use the provided client to execute the query
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

    # Prepare the query to create inner data table in ClickHouse
    sql_ch_create_forecast_data_table = gen_sql_ch_create_forecast_data_table(ch_database)

    try:
        clickhouse_client.execute(sql_ch_create_forecast_data_table)
        logger.info(f"Local ClickHouse table '{local_table_name}' created successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to create local forecast_data table: {e}")
        return False

def insert_from_external(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    ch_database = clickhouse_config['database']

    sql_ch_insert_from_external = gen_sql_ch_insert_from_external(ch_database)

    try:
        clickhouse_client.execute(sql_ch_insert_from_external)
        logger.info("Bulk inserted data from external_pg_forecast_data into forecast_data.")
        return True
    except Exception as e:
        logger.error(f"Failed to insert data: {e}")
        return False