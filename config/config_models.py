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
#    'arima': {
#        'training_dataset_size': 240,
#        'model_update_interval': 2880,
#        'forecast_dataset_size': 48,
#        'forecast_frequency': 120,
#        'forecast_hours': 120,
#        'fit_func_name': 'models.model_fits.fit_arima_model',
#        'specific_parameters': {
#            'order': (24, 3, 8),
#            'seasonal_order': (0, 0, 0, 0)
#        },
#        'model_alg_name': 'arima'
#    },
    'ets': {
        'training_dataset_size': 480,
        'model_update_interval': 24,
        'forecast_dataset_size': 48,
        'forecast_frequency': 24,
        'forecast_hours': 96,
        'fit_func_name': 'models.model_fits.fit_ets_model',
        'specific_parameters': {
            'trend': 'add',
            'seasonal': 'mul',
            'seasonal_periods': 24
        },
        'model_alg_name': 'ets'
    },
    'theta': {
        'training_dataset_size': 480,
        'model_update_interval': 24,
        'forecast_dataset_size': 48,
        'forecast_frequency': 24,
        'forecast_hours': 96,
        'fit_func_name': 'models.model_fits.fit_theta_model',
        'specific_parameters': {},
        'model_alg_name': 'theta'
    }
}