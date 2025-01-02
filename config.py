#  Project config
CRYPTO_LIST = ['BTC', 'ETH'] #, 'DOGE', 'ADA', 'SOL']
START_DATE = "2024-12-26T00:00:00" # first day and hour of historical data in naive.
FINISH_DATE = "NOW" # last day and hour of historical data in naive, "yyyy-mm-ddThh:mm:ss" format.
API_KEY = "YOUR_API_KEY" # not hidden, cause it is not nessesarry for free requests. Implemented in code for the case of future changes.
# ---------------------------------------------------------
# Database configuration.
# It is empty, as it is using environmental variables with higher priority:
#FORECRYPT_DB_NAME=forecrypt_db
#FORECRYPT_DB_USER=SET_YOUR_USER
#FORECRYPT_DB_PASS=SET_YOUR_PASS
#FORECRYPT_DB_HOST=localhost
#FORECRYPT_DB_PORT=5432
#FORECRYPT_API_KEY=YOUR_API_KEY
# You can create your .env file in project's directory. Check README.md.
# Or just enter your database config below, if security doesn't matter much:
DB_CONFIG = {
    'dbname': '', 
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': 5432
}
# ---------------------------------------------------------
# Directory for check, save and load models. Mostly used in models_processing.py
MODELS_DIRECTORY = r"C:\forecrypt_models"
# ---------------------------------------------------------
# Model parameters. 
#
#
#
#
#
#
#
#

MODEL_PARAMETERS = {
    'arima': {
        'training_dataset_size': 720,
        'model_update_interval': 720,
        'forecast_dataset_size': 48,
        'forecast_frequency': 12,
        'forecast_hours': 120,
        'balanced_forecast_hours': 10,
        'trash_forecast_hours': 20,
        'fit_func_name': 'model_fits.fit_arima_model',
        'specific_parameters': {
            'order': (24, 3, 8),
            'seasonal_order': (0, 0, 0, 0)
        }
    },
    'ets': {
        'training_dataset_size': 720,
        'model_update_interval': 48,
        'forecast_dataset_size': 48,
        'forecast_frequency': 12,
        'forecast_hours': 120,
        'balanced_forecast_hours': 15,
        'trash_forecast_hours': 15,
        'fit_func_name': 'model_fits.fit_ets_model',
        'specific_parameters': {
            'trend': 'add',
            'seasonal': 'mul',
            'seasonal_periods': 24
        }
    },
    'theta': {
        'training_dataset_size': 720,
        'model_update_interval': 1,
        'forecast_dataset_size': 48,
        'forecast_frequency': 12,
        'forecast_hours': 120,
        'balanced_forecast_hours': 20,
        'trash_forecast_hours': 10,
        'fit_func_name': 'model_fits.fit_theta_model',
        'specific_parameters': {}
    }
}
# ---------------------------------------------------------
# Data update interval for sheduler
HISTORICAL_UPDATE_TIME = {"hour": 0, "minute": 0}  # every day at 00:00
FORECAST_UPDATE_TIME = {"minute": 0}  # every hour at xx:00
