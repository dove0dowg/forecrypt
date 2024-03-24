import pandas as pd
import json
from data import get_data as g_d
from models import arima_model as a_m, ets_model as ets_m, theta_model as th_m

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

api_key = '7db9598cf3ebf01148dda37d500a35844ef435670b3d962a53f6d0a397c98d43'
hours = 720
crypto_ids = ['BNB', 'SOL', 'XRP']

crypto_dfs = {}
for crypto_id in crypto_ids:
    df = g_d.fetch_historical_data(crypto_id, hours, api_key)
    if 'date' in df.columns and not df.index._is_all_dates:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')
    crypto_dfs[crypto_id] = df

model_funcs = {
    'arima': a_m.fit_arima_model,
    'ets': ets_m.fit_ets_model,
    'theta': th_m.fit_theta_model
}

forecast_jsons = {}
for crypto_id, crypto_df in crypto_dfs.items():
    for model_name, model_func in model_funcs.items():
        # Fit the model
        model_fit = model_func(crypto_df)
        # Create the forecast JSON, including the model name
        forecast_key = f'{crypto_id}_{model_name}'
        forecast_jsons[forecast_key] = create_forecast_json(crypto_df['price'], model_fit, 30, model_name=forecast_key)
for key in forecast_jsons.keys():
    print(forecast_jsons[key])