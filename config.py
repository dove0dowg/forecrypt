#  Project config
CRYPTO_LIST = ['BTC', 'ETH'] #, 'DOGE', 'ADA', 'SOL']
START_DATE = "2025-01-01T00:00:00" # first day and hour of historical data in naive.
FINISH_DATE = "NOW" # last day and hour of historical data in naive, "yyyy-mm-ddThh:mm:ss" format.
API_KEY = "YOUR_API_KEY" # not hidden, cause it is not nessesarry for free requests. Implemented in code for the case of future changes.
# ---------------------------------------------------------
# Database configuration.
# It is empty, as ForecrypT using environmental variables with higher priority:
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

# 'model_name' (e.g. 'arima') - Name of the model. This name is mostly used for tagging in logs and the database.
#                               It can be customized to suit your project needs. Note: processing is determined by `fit_func_name`.
# 'training_dataset_size': 720, - Number of hours of historical data used for training the model.
#                                 Ensure this value matches the model's requirements for accuracy.
# 'model_update_interval': 240, - Time interval (in hours) between model retrainings.
#                                 If the last retraining was more than this value ago, the model will retrain.
# 'forecast_dataset_size': 48, - Number of hours of historical data required for forecasting.
#                                This dataset is passed to the model for generating predictions.
# 'forecast_frequency': 12, - Frequency (in hours) for generating forecasts.
#                              If the last forecast was created more than this value ago, a new forecast will be generated.
# 'forecast_hours': 120, - Total number of hours the model should forecast.
#                          This determines the length of the prediction output.
# 'fit_func_name': 'model_fits.fit_arima_model', - Path to the function responsible for fitting the model.
#                                                 This function is dynamically imported and executed.
# 'specific_parameters': - Model-specific hyperparameters required for training.
#                          These parameters vary between models (e.g., ARIMA orders, ETS seasonal periods, etc.).

MODEL_PARAMETERS = {
    'arima': {
        'training_dataset_size': 120,
        'model_update_interval': 1440,
        'forecast_dataset_size': 48,
        'forecast_frequency': 12,
        'forecast_hours': 120,
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
        'fit_func_name': 'model_fits.fit_ets_model',
        'specific_parameters': {
            'trend': 'add',
            'seasonal': 'mul',
            'seasonal_periods': 24
        }
    },
    'theta': {
        'training_dataset_size': 240,
        'model_update_interval': 1,
        'forecast_dataset_size': 48,
        'forecast_frequency': 12,
        'forecast_hours': 120,
        'fit_func_name': 'model_fits.fit_theta_model',
        'specific_parameters': {}
    }
}
# ---------------------------------------------------------
# Data update interval for sheduler
HISTORICAL_UPDATE_TIME = {"hour": 0, "minute": 0}  # every day at 00:00
FORECAST_UPDATE_TIME = {"minute": 0}  # every hour at xx:00
