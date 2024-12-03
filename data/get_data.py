import requests
import pandas as pd
from datetime import datetime, timezone

def fetch_historical_data(crypto_id, hours, api_key=None):
    """
    Fetch historical data for the last `hours` hours and return it as a pandas DataFrame.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    params = {
        'fsym': crypto_id,
        'tsym': 'USD',
        'limit': hours - 1,  # limit includes current hour
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('Data', {}).get('Data', [])
        if data:
            df = pd.DataFrame(data)
            # convert timestamp to pandas datetime with UTC
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
            return pd.DataFrame({
                "date": df['time'],  # already datetime64[ns, UTC]
                "price": df['close']
            })
    print(f"Failed to fetch historical data for {crypto_id}. Status code: {response.status_code}")
    return pd.DataFrame()

def fetch_specific_historical_hours(crypto_id, hours_list, api_key=None):
    """
    Fetch data for specific hours from CryptoCompare API and return it as a pandas DataFrame.
    The input hours_list must contain `datetime` objects.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    results = []

    for hour in hours_list:  # hours_list contains `datetime` objects
        # Convert datetime to UNIX timestamp for API request
        hour_unix = int(hour.timestamp())
        params = {
            "fsym": crypto_id,
            "tsym": 'USD',
            "limit": 1,
            "toTs": hour_unix
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'Success':
                price_data = data['Data']['Data'][0]
                # Append data in a simplified format
                results.append({
                    "date": pd.Timestamp(price_data['time'], unit='s', tz='UTC'),  # direct pandas timestamp
                    "price": price_data['close']
                })
        else:
            print(f"Failed to fetch data for {crypto_id} at timestamp {hour}. Status code: {response.status_code}")

    # Return all results as a DataFrame
    return pd.DataFrame(results)
