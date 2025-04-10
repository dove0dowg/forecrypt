#  Project config
CRYPTO_LIST = ['BTC', 'ETH'] #, 'DOGE', 'ADA', 'SOL']
START_DATE = "2025-04-01T00:00:00" # first day and hour of historical data in naive.
FINISH_DATE = "2025-04-03T00:00:00" # last day and hour of historical data in naive, "yyyy-mm-ddThh:mm:ss" format.
# ---------------------------------------------------------
# Directory for check, save and load models. Mostly used in models_processing.py
MODELS_DIRECTORY = r"C:\forecrypt_models"
# ---------------------------------------------------------
#   Postgres database configuration.
#    It is empty, as ForecrypT using environmental variables with higher priority:
#    FORECRYPT_PG_DB_NAME=forecrypt_db
#    FORECRYPT_PG_DB_USER=SET_YOUR_USER
#    FORECRYPT_PG_DB_PASS=SET_YOUR_PASS
#    FORECRYPT_PG_DB_HOST=localhost
#    FORECRYPT_PG_DB_PORT=5433
#    You can create your .env file in project's directory. Check README.md.
#    Or just enter your database config below, if security doesn't matter much:
# ---------------------------------------------------------
PG_DB_CONFIG = {
    'dbname': '',  # database name
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5433,       # not standart 5432 to avoid connection to native Windows Postgres
    'container_name': 'postgres_container',
}
# ---------------------------------------------------------
#   Clickhouse database configuration.
#    It is empty, as ForecrypT using environmental variables with higher priority:
#    FORECRYPT_CH_DB_NAME=
#    FORECRYPT_CH_DB_USER=SET_YOUR_USER
#    FORECRYPT_CH_DB_PASS=SET_YOUR_PASS
#    FORECRYPT_CH_DB_HOST=localhost
#    FORECRYPT_CH_DB_PORT=
#    You can create your .env file in project's directory. Check README.md.
#    Or just enter your database config below, if security doesn't matter much:
# ---------------------------------------------------------
CH_DB_CONFIG = {
    'database': '', 
    'table': '',
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 9000,
    'http_port': 8123,
    'interserver_port': 9009,
    'container_name': 'clickhouse_container',
    'default_user_xml_path': '/etc/clickhouse-server/users.d/default-user.xml',
    'users_xml_path': '/etc/clickhouse-server/users.xml',
    'db_data_wsl_dir': '/var/lib/clickhouse/data',  #
    'config_wsl_dir': '/var/lib/clickhouse/config',  #
}
# ---------------------------------------------------------
# CryptoCompare API key. Not hidden, cause it is not nessesarry for free requests. Implemented for the case of future changes.
API_KEY = "YOUR_API_KEY" 
# ---------------------------------------------------------
# 
CORE_COLUMNS = {
    "id": ("UUID", "String"),
    "timestamp": ("TIMESTAMP", "DateTime"),
    "currency": ("VARCHAR(50)", "String"),
    "forecast_step": ("INTEGER", "Int32"),
    "forecast_value": ("DECIMAL(18, 8)", "Float64"),
    "historical_value": ("DECIMAL(18, 8)", "Float64"),
    "model": ("VARCHAR(100)", "String"),
    "model_name_ext": ("VARCHAR(100)", "String"),
    "external_model_params": ("VARCHAR(200)", "String"),
    "inner_model_params": ("VARCHAR(200)", "String"),
    "zero_step_ts": ("TIMESTAMP", "DateTime"),
    "config_start": ("TIMESTAMP", "DateTime"),
    "config_end": ("TIMESTAMP", "DateTime"),
    "data_label": ("VARCHAR(20)", "String"),
    "uploaded_at": ("TIMESTAMP", "DateTime")
}