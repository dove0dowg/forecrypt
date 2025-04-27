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

logger = logging.getLogger(__name__)

def initialize_environment(pg_config: dict, ch_config: dict) -> bool:
    load_dotenv(override=True)
    try:
        pg_ok = prepare_postgres()
        pg_cfg = update_pg_config(pg_config)

        ch_ok = prepare_clickhouse()
        ch_cfg = update_ch_config(ch_config)
        ch_cli = clickhouse_connection(ch_cfg)

        ext_ok = create_external_pg_table(ch_cli, ch_cfg, pg_cfg)
        local_ok = create_ch_forecast_data_table(ch_cli, ch_cfg)
        metr_ok = create_ch_metrics_tables(ch_cli, ch_cfg)

        return all([pg_ok, ch_ok, ext_ok, local_ok, metr_ok])
    except Exception as e:
        logger.exception("initialize_environment failed")
        return False

def prepare_pg_container() -> bool:
    load_dotenv(override=True)
    try:
        return prepare_postgres()
    except Exception as e:
        logger.exception("prepare_pg_container failed")
        return False

def prepare_ch_container() -> bool:
    load_dotenv(override=True)
    try:
        return prepare_clickhouse()
    except Exception as e:
        logger.exception("prepare_ch_container failed")
        return False

def prepare_pg_tables(pg_config: dict) -> bool:
    load_dotenv(override=True)
    try:
        pg_cfg = update_pg_config(pg_config)
        pg_conn = postgres_connection(**pg_cfg)

        logger.info("Postgres table creation is not yet implemented.")
        return True
    except Exception as e:
        logger.exception("prepare_pg_tables failed")
        return False

def prepare_ch_tables(pg_config: dict, ch_config: dict) -> bool:
    load_dotenv(override=True)
    try:
        ch_cfg = update_ch_config(ch_config)
        ch_cli = clickhouse_connection(ch_cfg)
        pg_cfg = update_pg_config(pg_config)

        ext_ok = create_external_pg_table(ch_cli, ch_cfg, pg_cfg)
        local_ok = create_ch_forecast_data_table(ch_cli, ch_cfg)
        metr_ok = create_ch_metrics_tables(ch_cli, ch_cfg)

        return all([ext_ok, local_ok, metr_ok])
    except Exception as e:
        logger.exception("prepare_ch_tables failed")
        return False

def clear_all_databases(pg_config: dict, ch_config: dict) -> bool:
    try:
        pg_cfg = update_pg_config(pg_config)
        pg_conn = postgres_connection(**pg_cfg)
        pg_ok = delete_mv_and_tables(pg_conn)

        ch_cfg = update_ch_config(ch_config)
        ch_cli = clickhouse_connection(ch_cfg)
        ch_ok = drop_clickhouse_database(ch_cli, ch_cfg)

        return pg_ok and ch_ok
    except Exception as e:
        logger.exception(f"clear_all_databases failed: {e}")
        return False

def run_full_cycle(pg_config: dict, ch_config: dict, model_params_dict: dict, start_date, finish_date, crypto_list: dict) -> bool:
    try:
        pg_cfg = update_pg_config(pg_config)
        pg_conn = postgres_connection(**pg_cfg)

        ch_cfg = update_ch_config(ch_config)
        ch_cli = clickhouse_connection(ch_cfg)

        step1 = fetch_predict_upload_ts(pg_conn, model_params_dict, start_date, finish_date, crypto_list)
        step2 = refresh_materialized_view(pg_conn)
        step3 = insert_from_external(ch_cli, ch_cfg)
        step4 = insert_ch_metrics(ch_cli, ch_cfg)

        logger.debug([step1, step2, step3, step4])
        return all([step1, step2, step3, step4])
    except Exception as e:
        logger.exception("run_full_cycle failed")
        return False
