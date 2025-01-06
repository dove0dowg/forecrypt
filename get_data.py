import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

def fetch_historical_data(crypto_id, hours, api_key=None):
    """
    Fetch historical hourly cryptocurrency data from an external API with support for pagination.
    
    :param crypto_id: The cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param hours: The total number of hours of historical data to fetch.
    :param api_key: API key for authentication (optional).
    :return: A pandas DataFrame with columns ['date', 'price'] and a DatetimeIndex (naive, hourly frequency).
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    all_data = []
    limit = 2000  # CryptoCompare API limit per request

    params = {
        'fsym': crypto_id,
        'tsym': 'USD',
        'limit': limit - 1  # API returns 'limit + 1' points, so reduce by 1
    }

    while hours > 0:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json().get('Data', {}).get('Data', [])
            if not data:
                break  # Stop if no data is returned

            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['time'], unit='s')  # naive datetime
            all_data.append(df)

            hours -= len(df)  # Reduce remaining hours
            last_timestamp = df['time'].iloc[0]  # Get earliest timestamp for next request
            params['toTs'] = last_timestamp  # Set the new 'toTs' for the next request
        else:
            print(f"Failed to fetch data for {crypto_id}. Status code: {response.status_code}")
            break

    if not all_data:
        return pd.DataFrame()  # Return empty DataFrame if no data was fetched

    # Concatenate all partial DataFrames and remove duplicate timestamps if any
    full_df = pd.concat(all_data).drop_duplicates(subset='date').sort_values(by='date')
    full_df.set_index('date', inplace=True)
    full_df = full_df.asfreq('h') # set DatetimeIndex to hourly

    return pd.DataFrame({"date": full_df.index, "price": full_df['close']})

def fetch_specific_historical_hours(crypto_id, hours_list, api_key=None):
    """
    Fetch specific hourly cryptocurrency data for a list of timestamps from an external API.
    
    :param crypto_id: The cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param hours_list: A list of timestamps (datetime objects) for which to fetch data.
    :param api_key: API key for authentication (optional).
    :return: A pandas DataFrame with columns ['date', 'price'] containing the fetched data.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    results = []

    for hour in hours_list:
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
                price_data = data['Data']['Data'][1]
                results.append({
                    "date": pd.Timestamp(price_data['time'], unit='s'),# tz='UTC'),
                    "price": price_data['close']
                })
        else:
            print(f"Failed to fetch data for {crypto_id} at {hour}. Status code: {response.status_code}")
    print(results)
    return pd.DataFrame(results)