#  project config
CRYPTO_LIST = ['BTC', 'ETH'] #, 'DOGE', 'ADA', 'SOL']
START_DATE = "2024-12-17T00:00:00+00:00"   # first day and hour of analysis in UTC-timezone
API_KEY = "YOUR_API_KEY"

DB_CONFIG = {
    'dbname': 'forecrypt_db',
    'user': 'postgres',
    'password': 'forecrypt_db_pass',
    'host': 'localhost',
    'port': 5432
}

MODELS_DIRECTORY = r"C:\forecrypt_models"

MODEL_PARAMETERS = {
    'arima': {
        'dataset_hours': 720,
        'model_update_interval': 12,
        'forecast_hours': 30,
        'balanced_forecast_hours': 10,
        'trash_forecast_hours': 20,
        'fit_func_name': 'model_fits.fit_arima_model',
        'specific_parameters': {
            'order': (24, 3, 8),
            'seasonal_order': (0, 0, 0, 0)
        }
    },
    'ets': {
        'dataset_hours': 720,
        'model_update_interval': 12,
        'forecast_hours': 30,
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
        'dataset_hours': 720,
        'model_update_interval': 12,
        'forecast_hours': 30,
        'balanced_forecast_hours': 20,
        'trash_forecast_hours': 10,
        'fit_func_name': 'model_fits.fit_theta_model',
        'specific_parameters': {}
    }
}



# data update interval
HISTORICAL_UPDATE_TIME = {"hour": 0, "minute": 0}  # every day at 00:00
FORECAST_UPDATE_TIME = {"minute": 0}  # every hour at xx:00
