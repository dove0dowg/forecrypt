from clickhouse_driver import Client
from .core_columns_generators import (
    gen_sql_ch_create_pointwise_metrics_table,
    gen_sql_ch_insert_pointwise_metrics,
    gen_sql_ch_create_aggregated_metrics_table,
    gen_sql_ch_insert_aggregated_metrics
)

import logging

# from db.db_utils_clickhouse import update_ch_config, clickhouse_connection

logger = logging.getLogger(__name__)

def create_pointwise_metrics_table(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Creates the pointwise_metrics table in ClickHouse using CORE_COLUMNS and a fixed set of metric columns.
    The database name is extracted from the provided ClickHouse configuration.

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Creating pointwise_metrics table...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    create_table_sql = gen_sql_ch_create_pointwise_metrics_table(ch_database_name)

    try:
        clickhouse_client.execute(create_table_sql)
        logger.info("pointwise_metrics table created successfully.")
    except Exception as e:
        logger.exception(f"Failed to create pointwise_metrics table: {e}")
        raise

def insert_pointwise_metrics(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Inserts calculated pointwise metrics into ClickHouse from the forecast_data table.
    Only new records are inserted based on f_uploaded_at > max(pm_insert_time).

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Inserting pointwise metrics...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    insert_sql = gen_sql_ch_insert_pointwise_metrics(ch_database_name)

    try:
        clickhouse_client.execute(insert_sql)
        logger.info("Pointwise metrics inserted successfully.")
    except Exception as e:
        logger.exception(f"Failed to insert pointwise metrics: {e}")
        raise

def create_aggregated_metrics_table(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Creates the aggregated_metrics table in ClickHouse using CORE_COLUMNS and fixed aggregated metric columns.
    The database name is extracted from the provided ClickHouse configuration.

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Creating aggregated_metrics table...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    create_table_sql = gen_sql_ch_create_aggregated_metrics_table(ch_database_name)

    try:
        clickhouse_client.execute(create_table_sql)
        logger.info("aggregated_metrics table created successfully.")
    except Exception as e:
        logger.exception(f"Failed to create aggregated_metrics table: {e}")
        raise

def insert_aggregated_metrics(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Inserts aggregated metrics from pointwise_metrics into aggregated_metrics,
    grouped by (currency, model_name_ext). Only new records are inserted based on
    pm_insert_time > max(am_insert_time).

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Inserting aggregated metrics...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    insert_sql = gen_sql_ch_insert_aggregated_metrics(ch_database_name)

    try:
        clickhouse_client.execute(insert_sql)
        logger.info("Aggregated metrics inserted successfully.")
    except Exception as e:
        logger.exception(f"Failed to insert aggregated metrics: {e}")
        raise