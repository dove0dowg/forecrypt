import logging
from dotenv import load_dotenv

from db.db_utils_postgres import (
    prepare_postgres, update_pg_config, postgres_connection,
    delete_mv_and_tables, refresh_materialized_view,
)
from db.db_utils_clickhouse import (
    prepare_clickhouse, update_ch_config, clickhouse_connection,
    drop_clickhouse_database,
)
from db.pg_to_ch_pipeline import create_external_pg_table, create_ch_forecast_data_table, insert_from_external
from db.clickhouse_metrics import create_ch_metrics_tables, insert_ch_metrics
from models.df_and_models_engine import fetch_predict_upload_ts
from config.config_system import PG_DB_CONFIG, CH_DB_CONFIG

logger = logging.getLogger(__name__)

def initialize_environment() -> bool:
    """
    1) load env
    2) prepare Postgres (container, DB, tables)
    3) prepare ClickHouse (container, DB, tables)
    Returns True if everything OK, False otherwise.
    """
    load_dotenv(override=True)
    try:
        pg_ok = prepare_postgres()
        pg_cfg = update_pg_config(PG_DB_CONFIG)
        #pg_conn = postgres_connection(**pg_cfg)

        ch_ok = prepare_clickhouse()
        ch_cfg = update_ch_config(CH_DB_CONFIG)
        ch_cli = clickhouse_connection(ch_cfg)

        ext_ok = create_external_pg_table(ch_cli, ch_cfg, pg_cfg)
        local_ok = create_ch_forecast_data_table(ch_cli, ch_cfg)
        metr_ok = create_ch_metrics_tables(ch_cli, ch_cfg)

        return all([pg_ok, ch_ok, ext_ok, local_ok, metr_ok])
    except Exception as e:
        logger.exception("initialize_environment failed")
        return False

def prepare_pg_container() -> bool:
    """
    Setup the Postgres container and basic connection without creating tables.
    """
    load_dotenv(override=True)
    try:
        result = prepare_postgres()
        return result
    except Exception as e:
        logger.exception("prepare_pg_container failed")
        return False

def prepare_ch_container() -> bool:
    """
    Setup the ClickHouse container and user, without creating database/tables.
    """
    load_dotenv(override=True)
    try:
        result = prepare_clickhouse()
        return result
    except Exception as e:
        logger.exception("prepare_ch_container failed")
        return False

def prepare_pg_tables() -> bool:
    """
    Create materialized view and tables in Postgres.
    """
    load_dotenv(override=True)
    try:
        pg_cfg = update_pg_config(PG_DB_CONFIG)
        pg_conn = postgres_connection(**pg_cfg)

        # TODO: replace with actual table/view creation
        logger.info("Postgres table creation is not yet implemented.")
        return True
    except Exception as e:
        logger.exception("prepare_pg_tables failed")
        return False

def prepare_ch_tables() -> bool:
    """
    Create ClickHouse external table, local forecast table, and metric tables.
    """
    load_dotenv(override=True)
    try:
        ch_cfg = update_ch_config(CH_DB_CONFIG)
        ch_cli = clickhouse_connection(ch_cfg)
        pg_cfg = update_pg_config(PG_DB_CONFIG)

        ext_ok = create_external_pg_table(ch_cli, ch_cfg, pg_cfg)
        local_ok = create_ch_forecast_data_table(ch_cli, ch_cfg)
        metr_ok = create_ch_metrics_tables(ch_cli, ch_cfg)

        return all([ext_ok, local_ok, metr_ok])
    except Exception as e:
        logger.exception("prepare_ch_tables failed")
        return False

def clear_all_databases() -> bool:
    """
    1) clear Postgres tables/view
    2) drop ClickHouse database
    Returns True if both succeed.
    """
    try:
        pg_cfg = update_pg_config(PG_DB_CONFIG)
        pg_conn = postgres_connection(**pg_cfg)
        pg_ok = delete_mv_and_tables(pg_conn)

        ch_cfg = update_ch_config(CH_DB_CONFIG)
        ch_cli = clickhouse_connection(ch_cfg)
        ch_ok = drop_clickhouse_database(ch_cli, ch_cfg)

        return pg_ok and ch_ok
    except Exception as e:
        logger.exception(f"clear_all_databases failed: {e}")
        return False

def run_full_cycle() -> bool:
    """
    1) fetch→forecast→upload into Postgres
    2) refresh materialized view
    3) transfer to ClickHouse and calculate metrics
    Returns True if all steps OK.
    """
    try:
        pg_cfg = update_pg_config(PG_DB_CONFIG)
        pg_conn = postgres_connection(**pg_cfg)

        ch_cfg = update_ch_config(CH_DB_CONFIG)
        ch_cli = clickhouse_connection(ch_cfg)

        step1 = fetch_predict_upload_ts(pg_conn)
        step2 = refresh_materialized_view(pg_conn)
        step3 = insert_from_external(ch_cli, ch_cfg)
        step4 = insert_ch_metrics(ch_cli, ch_cfg)

        logger.debug([step1, step2, step3, step4])
        return all([step1, step2, step3, step4])
    except Exception as e:
        logger.exception("run_full_cycle failed")
        return False