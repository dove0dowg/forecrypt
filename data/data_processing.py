import pandas as pd
import json

def create_forecast_json(crypto_df, model_fit, steps=31): 
    if isinstance(crypto_df, pd.DataFrame): # df-format check
        if 'price' not in crypto_df.columns: 
            raise ValueError("The DataFrame does not contain a 'price' column.")
        crypto_df = crypto_df['price'] 
    
    if isinstance(crypto_df.index, pd.PeriodIndex): # convert df-index to timestamps if it is PeriodIndex
        crypto_df.index = crypto_df.index.to_timestamp()
    
    if hasattr(model_fit, 'get_forecast'):  # for ARIMA, check attribute get_forecast
        forecast_result = model_fit.get_forecast(steps=steps-1)
        forecast = forecast_result.predicted_mean
    else:  # for ETS and Theta use ".forecast" method
        forecast = model_fit.forecast(steps=steps-1)
    # -1 offset to delete last historical point from forecast
    forecast_dates = pd.date_range(start=crypto_df.index[-1] + pd.Timedelta(hours=1), periods=steps-1, freq='h')    
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # dictionary for the forecast data
    forecast_dict = {index.strftime('%Y-%m-%dT%H:%M:%S'): value for index, value in forecast_series.items()}

    # convert dictionary to json-string
    json_str = json.dumps(forecast_dict)

    return json_str