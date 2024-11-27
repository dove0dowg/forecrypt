from fastapi import FastAPI, HTTPException
from data.model_parameters import model_params as m_p
from data.data_processing import create_forecast_json
from data.get_data import fetch_historical_data
import models
import pandas as pd
import importlib as il

app = FastAPI()

def load_function(func_path: str):
    module_name, func_name = func_path.rsplit('.', 1)
    module = il.import_module(module_name)
    return getattr(module, func_name)

@app.get("/historical/{crypto_id}/{hours}")
async def historical_data(crypto_id: str, hours: int):
    """
    Получение исторических данных для указанной криптовалюты.
    """
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
    """
    Получение прогноза на основе указанной модели для криптовалюты.
    """
    if model_name not in m_p:
        raise HTTPException(status_code=400, detail="Invalid model name")

    model_param = m_p[model_name]
    
    # Загружаем функцию динамически
    fit_func = load_function(model_param['fit_func_name'])
    
    # Получаем данные прогноза
    df = fetch_historical_data(crypto_id, model_param['dataset_hours'], api_key="YOUR_API_KEY")

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')

    model_fit = fit_func(df)  # Вызываем загруженную функцию
    forecast_json = create_forecast_json(df['price'], model_fit, 31)

    return {
        'forecast': forecast_json
    }

@app.get("/historical_and_forecast/{crypto_id}/{model_name}")
async def historical_and_forecast(crypto_id: str, model_name: str):
    """
    Получение как исторических данных, так и прогноза на основе модели.
    """
    if model_name not in m_p:
        raise HTTPException(status_code=400, detail="Invalid model name")

    model_param = m_p[model_name]
    
    # Загружаем функцию динамически
    fit_func = load_function(model_param['fit_func_name'])
    
    # Получаем исторические данные
    df = fetch_historical_data(crypto_id, model_param['dataset_hours'], api_key="YOUR_API_KEY")

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')

    # Получаем прогноз
    model_fit = fit_func(df)  # Вызываем загруженную функцию
    forecast_json = create_forecast_json(df['price'], model_fit, 31)

    return {
        'historical_and_forecast': {
            'historical_data': {index.strftime('%Y-%m-%dT%H:%M:%S'): value for index, value in df['price'].items()},
            'forecast': forecast_json
        }
    }