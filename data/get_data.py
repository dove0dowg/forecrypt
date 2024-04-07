import requests
import pandas as pd

# Function to get json from cryptocompare api and rebuild it in pandas dataframe
def fetch_historical_data(crypto_id, hours, api_key):
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    params = {
        'fsym': crypto_id,
        'tsym': 'USD',
        'limit': hours,
        'api_key': api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()['Data']['Data']
        df = pd.DataFrame(data)
        # Convert timestamp to datetime
        df['time'] = pd.to_datetime(df['time'], unit='s')
        return df[['time', 'close']].rename(columns={'time': 'date', 'close': 'price'})
    else:
        print(f"Failed to fetch data for {crypto_id}. Status code: {response.status_code}")
        return pd.DataFrame()

