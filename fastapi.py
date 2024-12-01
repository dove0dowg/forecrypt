from fastapi import FastAPI, HTTPException
from config import MODEL_PARAMETERS as m_p
from data.data_processing import create_forecast_json
from data.get_data import fetch_historical_data
import pandas as pd
import importlib as il
import json

# imports models folder, uses string paths from config.py to model fit functions
def load_fit_function(func_path: str):
    module_name, func_name = func_path.rsplit('.', 1)
    module = il.import_module(module_name)
    return getattr(module, func_name)

app = FastAPI()

@app.get("/historical/{crypto_id}/{hours}")
async def historical_data(crypto_id: str, hours: int):
    
    df = fetch_historical_data(crypto_id, hours, api_key="YOUR_API_KEY")
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')    

    if df.empty:
        raise HTTPException(status_code=400, detail="Failed to fetch historical data")

    return {
        'historical_data': {index.strftime('%Y-%m-%dT%H:%M:%S'): value for index, value in df['price'].items()}
    }

@app.get("/forecast/{crypto_id}/{model_name}")
async def forecast(crypto_id: str, model_name: str):

    if model_name not in m_p:
        raise HTTPException(status_code=400, detail="Invalid model name")

    model_param = m_p[model_name]
    
    # dynamic load of string path to model fit function
    fit_func = load_fit_function(model_param['fit_func_name'])
    
    # historical df call and pandas transform into timeseries format
    df = fetch_historical_data(crypto_id, model_param['dataset_hours'], api_key="YOUR_API_KEY")
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')

    model_fit = fit_func(df)  # call loaded fit function
    forecast_json_str = create_forecast_json(df['price'], model_fit, 31)

    # transform json string to json dictionary and round by 8
    forecast_json = {k: round(v, 8) for k, v in json.loads(forecast_json_str).items()}

    return {
        'forecast': forecast_json
    }

@app.get("/historical_and_forecast/{crypto_id}/{model_name}")
async def historical_and_forecast(crypto_id: str, model_name: str):
    if model_name not in m_p:
        raise HTTPException(status_code=400, detail="Invalid model name")

    model_param = m_p[model_name]
    
    # dynamic load of string path to model fit function
    fit_func = load_fit_function(model_param['fit_func_name'])
    
    # historical df
    df = fetch_historical_data(crypto_id, model_param['dataset_hours'], api_key="YOUR_API_KEY")
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')

    model_fit = fit_func(df)  # call loaded fit function
    forecast_json_str = create_forecast_json(df['price'], model_fit, 31)

    # transform json string to json dictionary and round by 8
    forecast_json = {k: round(v, 8) for k, v in json.loads(forecast_json_str).items()}

    return {
        'historical_and_forecast': {
            'historical_data': {index.strftime('%Y-%m-%dT%H:%M:%S'): value for index, value in df['price'].items()},
            'forecast': forecast_json
        }
    }
