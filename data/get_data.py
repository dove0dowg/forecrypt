import requests
import pandas as pd
from datetime import datetime, timezone

def fetch_historical_data(crypto_id, hours, api_key=None):
    """
    Get json from cryptocompare api and rebuild it in pandas df
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
            # convert timestamp to datetime
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
            return pd.DataFrame({
                "date": df['time'],
                "price": df['close']
            })
    print(f"Failed to fetch historical data for {crypto_id}. Status code: {response.status_code}")
    return pd.DataFrame()


def fetch_specific_hours(crypto_id, hours_list, api_key=None):
    """
    Get jsons for specific hours from cryptocompare api and rebuild them into single pandas df
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    results = []

    for hour in hours_list:
        # ensure hour is a string in the correct format
        if not isinstance(hour, str):
            hour = hour.strftime("%Y-%m-%d %H:%M:%S")

        # convert timestring into UNIX timestamp
        hour_unix = int(datetime.strptime(hour, "%Y-%m-%d %H:%M:%S").timestamp())
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
                # combine df
                results.append({
                    "date": datetime.fromtimestamp(price_data['time'], tz=timezone.utc),
                    "price": price_data['close']
                })
        else:
            print(f"Failed to fetch data for {crypto_id} at {hour}. Status code: {response.status_code}")

    return pd.DataFrame(results)