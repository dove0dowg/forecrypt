import requests
import pandas as pd
import logging

# initialize logger
logger = logging.getLogger(__name__)

def fetch_historical_data(crypto_id, start_date, end_date, api_key=None):
    """
    Fetch historical hourly cryptocurrency data from an external API for a specific time range with support for pagination.

    :param crypto_id: The cryptocurrency identifier (e.g., 'BTC', 'ETH').
    :param start_date: The start date (inclusive) as a string in 'yyyy-mm-dd HH:MM:SS' format.
    :param end_date: The end date (inclusive) as a string in 'yyyy-mm-dd HH:MM:SS' format.
    :param api_key: API key for authentication (optional).
    :return: A pandas DataFrame with columns ['date', 'price'] and a DatetimeIndex (naive, hourly frequency).
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    all_data = []
    limit = 2000  # CryptoCompare API limit per request

    start_timestamp = int(pd.Timestamp(start_date).timestamp())
    end_timestamp = int(pd.Timestamp(end_date).timestamp())

    params = {
        'fsym': crypto_id,
        'tsym': 'USD',
        'limit': limit - 1,  # Adjust limit for the request
        'toTs': end_timestamp  # Start fetching from the end date backwards
    }

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json().get('Data', {}).get('Data', [])
            if not data:
                break  # Stop if no data is returned

            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['time'], unit='s')  # naive datetime
            all_data.append(df)

            last_timestamp = df['time'].iloc[0]
            if last_timestamp <= start_timestamp:
                break  # Stop if we've reached the start date

            params['toTs'] = last_timestamp  # Set the new 'toTs' for the next request
        else:
            logger.error(f"Failed to fetch data for {crypto_id}. Status code: {response.status_code}")
            break

    if not all_data:
        return pd.DataFrame()  # Return empty DataFrame if no data was fetched

    # Concatenate all partial DataFrames and remove duplicate timestamps if any
    full_df = pd.concat(all_data).drop_duplicates(subset='date', keep='first').sort_values(by='date')
    logger.debug(f"1 debug df call: {full_df.head()}")

    # Filter data to ensure it's within the requested date range
    full_df = full_df[(full_df['date'] >= pd.to_datetime(start_date)) & (full_df['date'] <= pd.to_datetime(end_date))]
    logger.debug(f"2 debug df call: {full_df.head()}")

    # Set index and keep 'date' as a column
    full_df.set_index('date', inplace=True)
    logger.debug(f"3 debug df call: {full_df.head()}")
    
    full_df.reset_index(drop=False, inplace=True)
    logger.debug(f"4 debug df call: {full_df.head()}")
    return pd.DataFrame({"date": full_df['date'], "price": full_df['close']})

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
            logger.error(f"Failed to fetch data for {crypto_id} at {hour}. Status code: {response.status_code}")
    logger.error(results)
    return pd.DataFrame(results)