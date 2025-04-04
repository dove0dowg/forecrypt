#  Project config
CRYPTO_LIST = ['BTC', 'ETH'] #, 'DOGE', 'ADA', 'SOL']
START_DATE = "2025-03-01T00:00:00" # first day and hour of historical data in naive.
FINISH_DATE = "2025-03-05T00:00:00" # last day and hour of historical data in naive, "yyyy-mm-ddThh:mm:ss" format.
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
#    FORECRYPT_PG_DB_PORT=5432
#    You can create your .env file in project's directory. Check README.md.
#    Or just enter your database config below, if security doesn't matter much:
# ---------------------------------------------------------
PG_DB_CONFIG = {
    'dbname': '',  # database name
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5433,
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
#   Model parameters. Two letters like [TD] means technical abbreviation mostly used for model objects and forecast naming in database. 
#    'model_name' (e.g. 'arima') - [MN] - Name of the model. This name is mostly used for tagging in logs and the database.
#                                  It can be customized to suit your project needs. Note: processing is determined by `fit_func_name`.
#    'training_dataset_size': 720, - [TD] - Number of hours of historical data used for training the model.
#                                    Ensure this value matches the model's requirements for accuracy.
#    'model_update_interval': 240, - [MU] - Time interval (in hours) between model retrainings.
#                                    If the last retraining was more than this value ago, the model will retrain.
#    'forecast_dataset_size': 48, - [FD] - Number of hours of historical data required for forecasting.
#                                   This dataset is passed to the model for generating predictions.
#    'forecast_frequency': 12, - [FF] - Frequency (in hours) for generating forecasts.
#                                 If the last forecast was created more than this value ago, a new forecast will be generated.
#    'forecast_hours': 120, - [FH] - Total number of hours the model should forecast.
#                             This determines the length of the prediction output.
#    'fit_func_name': 'model_fits.fit_arima_model', - Path to the function responsible for fitting the model.
#                                                    This function is dynamically imported and executed.
#    'specific_parameters': - [SP] - Model-specific hyperparameters required for training.
#                             These parameters vary between models (e.g., ARIMA orders, ETS seasonal periods, etc.).
# ---------------------------------------------------------
MODEL_PARAMETERS = {
    'arima': {
        'training_dataset_size': 240,
        'model_update_interval': 2880,
        'forecast_dataset_size': 48,
        'forecast_frequency': 120,
        'forecast_hours': 120,
        'fit_func_name': 'model_fits.fit_arima_model',
        'specific_parameters': {
            'order': (24, 3, 8),
            'seasonal_order': (0, 0, 0, 0)
        }
    },
    'ets': {
        'training_dataset_size': 240,
        'model_update_interval': 4800,
        'forecast_dataset_size': 48,
        'forecast_frequency': 8760,
        'forecast_hours': 720,
        'fit_func_name': 'model_fits.fit_ets_model',
        'specific_parameters': {
            'trend': 'add',
            'seasonal': 'mul',
            'seasonal_periods': 24
        }
    },
    'theta': {
        'training_dataset_size': 240,
        'model_update_interval': 24,
        'forecast_dataset_size': 48,
        'forecast_frequency': 120,
        'forecast_hours': 120,
        'fit_func_name': 'model_fits.fit_theta_model',
        'specific_parameters': {}
    }
}
# ---------------------------------------------------------
# Data update interval for sheduler
HISTORICAL_UPDATE_TIME = {"hour": 0, "minute": 0}  # every day at 00:00
FORECAST_UPDATE_TIME = {"minute": 0}  # every hour at xx:00
# ---------------------------------------------------------
# CryptoCompare API key. Not hidden, cause it is (suddenly) not nessesarry for free requests. Implemented for the case of future changes.
API_KEY = "YOUR_API_KEY" 