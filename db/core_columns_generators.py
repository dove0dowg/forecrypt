from collections import namedtuple
from config.config_system import CORE_COLUMNS
from config.config_metrics import EPSILON
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
        "zero_step_ts",
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
        "zero_step_ts",
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
        "zero_step_ts",
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

def gen_sql_ch_create_pointwise_metrics_table(ch_database_name: str) -> str:

    core_columns = name_core_columns_tuple()

    base_cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "zero_step_ts"
    ]

    lines = []
    for col in base_cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")
        ch_type = core_columns[col].ch_type
        lines.append(f"{col} {ch_type}")

    lines += [
        "abs_error Float64",
        "bias_value Float64",
        "squared_error Float64",
        "ape Float64",
        "perc_error Float64",
        "log_error Float64",
        "rel_error Float64",
        "overprediction UInt8",
        "underprediction UInt8",
        "zero_crossed UInt8",
        "pm_insert_time DateTime"
    ]

    gen_sql_string = (
        f"CREATE TABLE IF NOT EXISTS {ch_database_name}.pointwise_metrics (\n    "
        + ",\n    ".join(lines)
        + "\n) ENGINE = MergeTree\n"
        "ORDER BY (timestamp, currency, model_name_ext, forecast_step);"
    )

    return gen_sql_string

def gen_sql_ch_insert_pointwise_metrics(ch_database: str) -> str:

    core_columns = name_core_columns_tuple()

    base_cols = [
        "id",
        "timestamp",
        "currency",
        "forecast_step",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "zero_step_ts"
    ]

    for col in base_cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")

    insert_cols = base_cols + [
        "abs_error",
        "bias_value",
        "squared_error",
        "ape",
        "perc_error",
        "log_error",
        "rel_error",
        "overprediction",
        "underprediction",
        "zero_crossed",
        "pm_insert_time"
    ]

    metric_exprs = [
        "abs(forecast_value - historical_value) AS abs_error",
        "(forecast_value - historical_value) AS bias_value",
        "pow(forecast_value - historical_value, 2) AS squared_error",
        f"abs(forecast_value - historical_value) / (abs(historical_value) + {EPSILON}) AS ape",
        f"forecast_value / (historical_value + {EPSILON}) AS perc_error",
        f"log(forecast_value + {EPSILON}) - log(historical_value + {EPSILON}) AS log_error",
        f"abs(forecast_value - historical_value) / greatest(abs(forecast_value), abs(historical_value), {EPSILON}) AS rel_error",
        "if(forecast_value > historical_value, 1, 0) AS overprediction",
        "if(forecast_value < historical_value, 1, 0) AS underprediction",
        "if(forecast_value * historical_value < 0, 1, 0) AS zero_crossed",
        "now() AS pm_insert_time"
    ]

    select_clause = ",\n    ".join(base_cols + metric_exprs)

    gen_sql_string = f"""
    INSERT INTO {ch_database}.pointwise_metrics
    (
        {", ".join(insert_cols)}
    )
    SELECT
        {select_clause}
    FROM {ch_database}.forecast_data
    WHERE f_uploaded_at > (
        SELECT coalesce(MAX(pm_insert_time), toDateTime('1970-01-01 00:00:00'))
        FROM {ch_database}.pointwise_metrics
    )
    """.strip()

    return gen_sql_string

def gen_sql_ch_create_aggregated_metrics_table(ch_database_name: str) -> str:

    core_columns = name_core_columns_tuple()

    base_cols = [
        "currency",
        "model_name_ext",
        "external_model_params",
        "inner_model_params"
    ]

    lines = []
    for col in base_cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")
        ch_type = core_columns[col].ch_type
        lines.append(f"{col} {ch_type}")

    lines += [
        "mae Float64",
        "mse Float64",
        "rmse Float64",
        "mape Float64",
        "bias_value_mean Float64",
        "stddev_bias_value Float64",

        "overprediction_rate Float64",
        "underprediction_rate Float64",

        "max_abs_error Float64",
        "max_ape Float64",

        "row_count UInt32",
        "am_insert_time DateTime"
    ]

    gen_sql_string = (
        f"CREATE TABLE IF NOT EXISTS {ch_database_name}.aggregated_metrics (\n    "
        + ",\n    ".join(lines)
        + "\n) ENGINE = MergeTree\n"
        "ORDER BY (currency, model_name_ext);"
    )

    return gen_sql_string

def gen_sql_ch_insert_aggregated_metrics(ch_database: str) -> str:

    gen_sql_string = f"""
    INSERT INTO {ch_database}.aggregated_metrics
    (
        currency,
        model_name_ext,
        external_model_params,
        inner_model_params,

        mae,
        mse,
        rmse,
        mape,
        bias_value_mean,
        stddev_bias_value,

        overprediction_rate,
        underprediction_rate,

        max_abs_error,
        max_ape,

        row_count,
        am_insert_time
    )
    SELECT
        currency,
        model_name_ext,
        any(external_model_params) AS external_model_params,
        any(inner_model_params) AS inner_model_params,

        avg(abs_error) AS mae,
        avg(squared_error) AS mse,
        sqrt(avg(squared_error)) AS rmse,
        avg(ape) AS mape,
        avg(bias_value) AS bias_value_mean,
        stddevPop(bias_value) AS stddev_bias_value,

        avg(overprediction) AS overprediction_rate,
        avg(underprediction) AS underprediction_rate,

        max(abs_error) AS max_abs_error,
        max(ape) AS max_ape,

        count() AS row_count,
        now() AS am_insert_time
    FROM {ch_database}.pointwise_metrics
    WHERE pm_insert_time > (
        SELECT coalesce(MAX(am_insert_time), toDateTime('1970-01-01 00:00:00'))
        FROM {ch_database}.aggregated_metrics
    )
    GROUP BY currency, model_name_ext
    """.strip()

    return gen_sql_string

def gen_sql_ch_create_forecast_w_metrics_table(ch_database_name: str) -> str:

    core_columns = name_core_columns_tuple()

    base_cols = [
        "currency",
        "model_name_ext",
        "external_model_params",
        "inner_model_params",
        "zero_step_ts",
        "forecast_step"
    ]

    lines = []
    for col in base_cols:
        if col not in core_columns:
            raise ValueError(f"missing column: {col}")
        ch_type = core_columns[col].ch_type
        lines.append(f"{col} {ch_type}")

    lines += [
        "cumulative_mae Float64",
        "cumulative_rmse Float64",
        "mean_bias_value Float64",
        "error_growth_rate Float64",
        "relative_step_error Float64",
        "is_reversal UInt8",
        "step_stddev Float64",
        "step_rank UInt32",
        "fwmv_insert_time DateTime"
    ]

    gen_sql_string = (
        f"CREATE TABLE IF NOT EXISTS {ch_database_name}.forecast_window_metrics (\n    "
        + ",\n    ".join(lines)
        + "\n) ENGINE = MergeTree\n"
        "ORDER BY (currency, model_name_ext, zero_step_ts, forecast_step);"
    )

    return gen_sql_string

def gen_sql_ch_insert_forecast_w_metrics(ch_database: str) -> str:

    gen_sql_string = f"""
        INSERT INTO {ch_database}.forecast_window_metrics
        SELECT
            currency,
            model_name_ext,
            external_model_params,
            inner_model_params,
            zero_step_ts,
            forecast_step,

            avg(abs_error) OVER (
                PARTITION BY currency, model_name_ext, zero_step_ts
                ORDER BY forecast_step
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_mae,

            sqrt(avg(squared_error) OVER (
                PARTITION BY currency, model_name_ext, zero_step_ts
                ORDER BY forecast_step
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) AS cumulative_rmse,

            avg(bias_value) OVER (
                PARTITION BY currency, model_name_ext, zero_step_ts
                ORDER BY forecast_step
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS mean_bias_value,

            bias_value / greatest(forecast_step, 1) AS error_growth_rate,
            abs_error / greatest(forecast_step, 1) AS relative_step_error,

            if(
                bias_value * leadInFrame(bias_value) OVER (
                    PARTITION BY currency, model_name_ext, zero_step_ts
                    ORDER BY forecast_step
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) < 0,
                1, 0
            ) AS is_reversal,

            stddevPop(bias_value) OVER (
                PARTITION BY currency, model_name_ext, zero_step_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS step_stddev,

            row_number() OVER (
                PARTITION BY currency, model_name_ext, zero_step_ts
                ORDER BY forecast_step
            ) AS step_rank,

            now() AS fwmv_insert_time
        FROM {ch_database}.pointwise_metrics
        WHERE pm_insert_time > (
            SELECT coalesce(MAX(fwmv_insert_time), toDateTime('1970-01-01 00:00:00'))
            FROM {ch_database}.forecast_window_metrics
        )
    """.strip()

    return gen_sql_string
