import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

def fetch_historical_data(crypto_id, hours, api_key=None):
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    params = {
        'fsym': crypto_id,
        'tsym': 'USD',
        'limit': hours - 1,
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('Data', {}).get('Data', [])
        if data:
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['time'], unit='s')#, utc=True)
            return pd.DataFrame({"date": df['date'], "price": df['close']})
    
    print(f"Failed to fetch data for {crypto_id}. Status code: {response.status_code}")
    return pd.DataFrame()

def fetch_specific_historical_hours(crypto_id, hours_list, api_key=None):
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    results = []

    for hour in hours_list:
        print(f'час:{hour}')
        #hour = hour.astimezone(timezone.utc)
        #print(f'час:{hour}')
        hour_unix = int(hour.timestamp())
        print(f'час_unix:{hour_unix}')
        params = {
            "fsym": crypto_id,
            "tsym": 'USD',
            "limit": 1,
            "toTs": hour_unix
        }

        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(data)
            if data.get('Response') == 'Success':
                price_data = data['Data']['Data'][1]
                results.append({
                    "date": pd.Timestamp(price_data['time'], unit='s'),# tz='UTC'),
                    "price": price_data['close']
                })
        else:
            print(f"Failed to fetch data for {crypto_id} at {hour}. Status code: {response.status_code}")
    print(results)
    return pd.DataFrame(results)