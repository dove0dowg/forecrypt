from collections import namedtuple
from config.config_system import CORE_COLUMNS
from uuid import uuid4
import logging

logger = logging.getLogger(__name__)


def name_core_columns_tuple():
    core_column_type = namedtuple("CoreColumnType", ["pg_type", "ch_type"])

    core_columns = {
        col: core_column_type(pg, ch)
        for col, (pg, ch) in CORE_COLUMNS.items()
    }

    logger.debug("core_columns initialized:")
    for col, types in core_columns.items():
        logger.debug(f"{col}: pg_type={types.pg_type}, ch_type={types.ch_type}")

    return core_columns

def gen_sql_pg_create_historical_table():
    cols = [
        "id",
        "timestamp",
        "currency",
        "historical_value",
        "data_label",
        "uploaded_at"
    ]

    core_columns = name_core_columns_tuple()

    lines = []
    for col in cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")
        pg_type = core_columns[col].pg_type
        lines.append(f"{col} {pg_type}")

    lines.append("UNIQUE (timestamp, currency, data_label)")

    gen_sql_string = (
        "CREATE TABLE historical_data (\n    "
        + ",\n    ".join(lines)
        + "\n);"
    )

    return gen_sql_string

def gen_sql_pg_create_historical_idx():
    gen_sql_string = (
        "CREATE INDEX IF NOT EXISTS idx_historical_data_timestamp_currency "
        "ON historical_data (timestamp, currency);"
    )
    return gen_sql_string

def gen_sql_pg_create_forecast_table():

    cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "forecast_value",
        "model",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "zero_step_ts",
        "config_start",
        "config_end",
        "uploaded_at"
    ]

    core_columns = name_core_columns_tuple()

    lines = []
    for col in cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")
        pg_type = core_columns[col].pg_type
        lines.append(f"{col} {pg_type}")

    lines.append("UNIQUE (timestamp, currency, model, forecast_step)")

    gen_sql_string = (
        "CREATE TABLE forecast_data (\n    "
        + ",\n    ".join(lines)
        + "\n);"
    )

    return gen_sql_string

def gen_sql_pg_create_forecast_idx():
    gen_sql_string = (
        "CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp "
        "ON forecast_data (timestamp, currency);\n"
        "CREATE INDEX IF NOT EXISTS idx_forecast_data_timestamp_currency_model "
        "ON forecast_data (timestamp, currency, model);"
    )
    return gen_sql_string

def gen_sql_pg_create_mv():

    gen_sql_string = (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS backtest_data_mv AS\n"
        "SELECT\n"
        "    f.id,\n"
        "    f.timestamp,\n"
        "    f.currency,\n"
        "    f.forecast_step,\n"
        "    f.forecast_value,\n"
        "    h.historical_value,\n"
        "    f.model,\n"
        "    f.model_name_ext,\n"
        "    f.external_model_params,\n"
        "    f.inner_model_params,\n"
        "    h.uploaded_at AS h_uploaded_at,\n"
        "    f.uploaded_at AS f_uploaded_at,\n"
        "    f.zero_step_ts,\n"
        "    f.config_start,\n"
        "    f.config_end\n"
        "FROM forecast_data f\n"
        "LEFT JOIN historical_data h ON f.timestamp = h.timestamp AND f.currency = h.currency\n"
        "WITH NO DATA;"
    )

    return gen_sql_string

def gen_records_pg_train_and_historical(row, crypto_id, uploaded_at):
    return (
        str(uuid4()),
        row["date"],
        crypto_id,
        row["price"],
        row["data_label"],
        uploaded_at
    )

def gen_sql_pg_load_train_and_historical():
    gen_sql_string = (
        "INSERT INTO historical_data (id, timestamp, currency, historical_value, data_label, uploaded_at)\n"
        "VALUES %s\n"
        "ON CONFLICT (timestamp, currency, data_label)\n"
        "DO UPDATE SET\n"
        "    historical_value = EXCLUDED.historical_value,\n"
        "    uploaded_at = EXCLUDED.uploaded_at\n"
        "WHERE historical_data.historical_value <> EXCLUDED.historical_value;"
    )
    return gen_sql_string

def gen_records_pg_forecast(row, crypto_id, model_name, dynamic_model_name, external_model_params, inner_model_params, zero_step_ts, config_start, config_end, uploaded_at):
    return (
        str(uuid4()),
        row["date"],
        crypto_id,
        row["step"],
        row["price"],
        model_name,
        dynamic_model_name,
        external_model_params,
        inner_model_params,
        zero_step_ts,
        config_start,
        config_end,
        uploaded_at
    )

def gen_sql_pg_load_forecast():
    gen_sql_string = (
        "INSERT INTO forecast_data (id, timestamp, currency, forecast_step, forecast_value, model, model_name_ext, external_model_params, inner_model_params, zero_step_ts, config_start, config_end, uploaded_at)\n"
        "VALUES %s\n"
        "ON CONFLICT (timestamp, currency, model, forecast_step)\n"
        "DO UPDATE SET\n"
        "    forecast_value = EXCLUDED.forecast_value,\n"
        "    zero_step_ts = EXCLUDED.zero_step_ts,\n"
        "    uploaded_at = EXCLUDED.uploaded_at\n"
        "WHERE forecast_data.forecast_value <> EXCLUDED.forecast_value;"
    )
    return gen_sql_string

def gen_sql_ch_create_external_pg_table(pg_container_ip, pg_database, pg_user, pg_password, ch_database_name, ch_table_name):
    core_columns = name_core_columns_tuple()

    cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "forecast_value",
        "historical_value",
        "model",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "h_uploaded_at",
        "f_uploaded_at",
        "config_start",
        "config_end"
    ]

    lines = []
    for col in cols:
        ch_type = core_columns[col].ch_type if col in core_columns else "DateTime"
        lines.append(f"{col} {ch_type}")

    gen_sql_string = (
        f"CREATE TABLE {ch_database_name}.{ch_table_name} (\n    "
        + ",\n    ".join(lines)
        + f"\n) ENGINE = PostgreSQL('{pg_container_ip}:5432', '{pg_database}', 'backtest_data_mv', '{pg_user}', '{pg_password}');"
    )

    return gen_sql_string

def gen_sql_ch_create_forecast_data_table(ch_database_name):
    
    core_columns = name_core_columns_tuple()

    cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "forecast_value",
        "historical_value",
        "model",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "h_uploaded_at",
        "f_uploaded_at"
    ]

    lines = []
    for col in cols:
        ch_type = core_columns[col].ch_type if col in core_columns else "DateTime"
        lines.append(f"{col} {ch_type}")

    gen_sql_string = (
        f"CREATE TABLE IF NOT EXISTS {ch_database_name}.forecast_data (\n    "
        + ",\n    ".join(lines)
        + "\n) ENGINE = ReplacingMergeTree(h_uploaded_at)\n"
        "PARTITION BY toYYYYMM(timestamp)\n"
        "ORDER BY (timestamp, currency, model, forecast_step);"
    )

    return gen_sql_string

def gen_sql_ch_insert_from_external(ch_database):
    cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "forecast_value",
        "historical_value",
        "model",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "h_uploaded_at",
        "f_uploaded_at"
    ]

    select_clause = ",\n    ".join(cols)

    gen_sql_string = (
        f"INSERT INTO {ch_database}.forecast_data\n"
        "SELECT\n"
        f"    {select_clause}\n"
        f"FROM {ch_database}.external_pg_forecast_data;"
    )

    return gen_sql_string