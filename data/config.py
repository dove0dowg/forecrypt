CRYPTO_IDS = ['BTC', 'ETH', 'DOGE', 'ADA', 'SOL']
API_KEY = "YOUR_API_KEY"

# data update interval
HISTORICAL_UPDATE_TIME = {"hour": 0, "minute": 0}  # every day at 00:00
FORECAST_UPDATE_TIME = {"minute": 0}  # every hour

# timezone?
#TIMEZONE = "UTC"

model_parameters = {
    'arima': {
        'dataset_hours': 720,
        'balanced_hours': 10,
        'trash_hours': 20,
        'fit_func_name': 'models.arima_model.fit_arima_model'
    },
    'ets': {
        'dataset_hours': 720,
        'balanced_hours': 15,
        'trash_hours': 15,
        'fit_func_name': 'models.ets_model.fit_ets_model'
    },
    'theta': {
        'dataset_hours': 720,
        'balanced_hours': 20,
        'trash_hours': 10,
        'fit_func_name': 'models.theta_model.fit_theta_model'
    }
}

