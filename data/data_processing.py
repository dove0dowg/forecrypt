import pandas as pd
import json

def create_forecast_json(crypto_df, model_fit, steps=30, model_name=''):
    if isinstance(crypto_df, pd.DataFrame):
        if 'price' not in crypto_df.columns:
            raise ValueError("The DataFrame does not contain a 'price' column.")
        crypto_df = crypto_df['price']
    
    if isinstance(crypto_df.index, pd.PeriodIndex):
        crypto_df.index = crypto_df.index.to_timestamp()
    
    if hasattr(model_fit, 'get_forecast'):  # ARIMA
        forecast_result = model_fit.get_forecast(steps=steps)
        forecast = forecast_result.predicted_mean
    else:  # ETS and Theta
        forecast = model_fit.forecast(steps=steps)

    if isinstance(forecast, pd.Series):
        forecast = forecast.iloc[1:]

    forecast_dates = pd.date_range(start=crypto_df.index[-1] + pd.Timedelta(hours=1), periods=steps-1, freq='h')    
    forecast_series = pd.Series(forecast.values, index=forecast_dates)
    
    combined_series = pd.concat([crypto_df, forecast_series])

    combined_data_dict = {'data': combined_series.tolist(), 'index': combined_series.index.strftime('%Y-%m-%dT%H:%M:%S').tolist(), 'model_name': model_name}
    
    json_str = json.dumps(combined_data_dict)
    
    return json_str