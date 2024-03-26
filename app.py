import pandas as pd
from fastapi import FastAPI

import data.data_processing as d_dp, data.get_data as d_gd
from models import arima_model as a_m, ets_model as ets_m, theta_model as th_m

app = FastAPI()

@app.get("/forecast/{crypto_id}")
async def get_combined_json_time_series(crypto_id: str):
    api_key = 'your_api_key'  # replace with your actual API key or use environment variable
    hours = 720

    # Fetch historical data for the given crypto_id
    df = d_gd.fetch_historical_data(crypto_id, hours, api_key)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')

    model_funcs = {
        'arima': a_m.fit_arima_model,
        'ets': ets_m.fit_ets_model,
        'theta': th_m.fit_theta_model
    }

    forecast_jsons = {}
    for model_name, model_func in model_funcs.items():
        # Fit the model
        model_fit = model_func(df)
        # Create the forecast JSON, including the model name
        forecast_jsons[f'{crypto_id}_{model_name}'] = d_dp.create_forecast_json(df['price'], model_fit, 30)
    
    return forecast_jsons