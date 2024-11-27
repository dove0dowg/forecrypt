from fastapi import FastAPI, HTTPException
from data.model_parameters import model_params as m_p
from data.data_processing import create_forecast_json
from data.get_data import fetch_historical_data
import pandas as pd

app = FastAPI()

@app.get("/forecast/{crypto_id}/{model_name}")
async def forecast(crypto_id: str, model_name: str):
    if model_name not in m_p:
        raise HTTPException(status_code=400, detail="Invalid model name")

    model_param = m_p[model_name]
    df = fetch_historical_data(crypto_id, model_param['dataset_hours'], api_key="YOUR_API_KEY")

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.asfreq('h')
    
    model_fit = model_param['model_fit_func'](df)
    forecast_json = create_forecast_json(df['price'], model_fit, 31)

    return {
        'forecast': forecast_json,
        'historical_data': {index.strftime('%Y-%m-%dT%H:%M:%S'): value for index, value in df['price'].items()}
    }
