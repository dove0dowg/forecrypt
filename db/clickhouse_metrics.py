from clickhouse_driver import Client
from .core_columns_generators import (
    gen_sql_ch_create_pointwise_metrics_table,
    gen_sql_ch_insert_pointwise_metrics,
    gen_sql_ch_create_aggregated_metrics_table,
    gen_sql_ch_insert_aggregated_metrics,
    gen_sql_ch_create_forecast_w_metrics_table,
    gen_sql_ch_insert_forecast_w_metrics
)

import logging

# logger
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# [Create metrics tables]
# ---------------------------------------------------------

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

def create_forecast_w_metrics_table(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Creates the forecast_step_window_metrics table in ClickHouse using CORE_COLUMNS
    and calculated window-based step-level metrics. The database name is extracted
    from the provided ClickHouse configuration.

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Creating forecast_step_window_metrics table...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    create_table_sql = gen_sql_ch_create_forecast_w_metrics_table(ch_database_name)

    try:
        clickhouse_client.execute(create_table_sql)
        logger.info("forecast_step_window_metrics table created successfully.")
    except Exception as e:
        logger.exception(f"Failed to create forecast_step_window_metrics table: {e}")
        raise
# final combined [Create metrics tables] function
def create_ch_metrics_tables(clickhouse_client: Client, clickhouse_config: dict) -> bool:
    """
    Creates all ClickHouse metrics tables (pointwise, aggregated and forecast-window).

    Args:
        clickhouse_client (Client): Active ClickHouse client.
        clickhouse_config (dict): Configuration dict containing at least 'database' key.

    Returns:
        bool: True if all tables were created successfully, False otherwise.
    """
    logger.info("Starting creation of ClickHouse metrics tables...")
    try:
        create_pointwise_metrics_table(clickhouse_client, clickhouse_config)
        create_aggregated_metrics_table(clickhouse_client, clickhouse_config)
        create_forecast_w_metrics_table(clickhouse_client, clickhouse_config)
        logger.info("All ClickHouse metrics tables created successfully.")
        return True
    except Exception as e:
        logger.exception(f"Error creating ClickHouse metrics tables: {e}")
        return False

# ---------------------------------------------------------
# [Insert into metrics tables]
# ---------------------------------------------------------

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

def insert_forecast_w_metrics(clickhouse_client: Client, clickhouse_config: dict) -> None:
    """
    Inserts step-level forecast window metrics with window functions
    from pointwise_metrics into forecast_window_metrics. Only new rows are inserted
    based on pm_insert_time > max(fsm_insert_time).

    Args:
        clickhouse_client (Client): Active ClickHouse connection.
        clickhouse_config (dict): ClickHouse configuration dictionary with a required 'database' key.
    """
    logger.info("Inserting forecast_window_metrics...")

    ch_database_name = clickhouse_config.get("database")
    if not ch_database_name:
        raise ValueError("Missing 'database' key in clickhouse_config")

    insert_sql = gen_sql_ch_insert_forecast_w_metrics(ch_database_name)

    try:
        clickhouse_client.execute(insert_sql)
        logger.info("forecast_window_metrics inserted successfully.")
    except Exception as e:
        logger.exception(f"Failed to insert forecast_window_metrics: {e}")
        raise
# final combined [Insert into metrics tables] function
def insert_ch_metrics(clickhouse_client: Client, clickhouse_config: dict) -> bool:
    """
    Inserts data into all ClickHouse metrics tables (pointwise, aggregated and forecast-window).

    Args:
        clickhouse_client (Client): Active ClickHouse client.
        clickhouse_config (dict): Configuration dict containing at least 'database' key.

    Returns:
        bool: True if all inserts succeeded, False otherwise.
    """
    logger.info("Starting insertion into ClickHouse metrics tables...")
    try:
        insert_pointwise_metrics(clickhouse_client, clickhouse_config)
        insert_aggregated_metrics(clickhouse_client, clickhouse_config)
        insert_forecast_w_metrics(clickhouse_client, clickhouse_config)
        logger.info("All ClickHouse metrics data inserted successfully.")
        return True
    except Exception as e:
        logger.exception(f"Error inserting into ClickHouse metrics tables: {e}")
        return False



