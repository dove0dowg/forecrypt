How to Set Up a Custom Time Series Source?
==========================================

To modify the time series source in Forecrypt, you simply need to rewrite the `fetch_historical_data` function. This function is responsible for retrieving the time series data and returning it as a pandas DataFrame in a specific format with an additional time index.

The goal is to ensure that your custom implementation provides a DataFrame with the required structure:

- **Columns**: `['date', 'price']`
- **Index**: A datetime index representing the hourly time points.

---

## Steps to Set Up a Custom Time Series Source

1. **Open the `get_data.py` module**:
   Locate the `fetch_historical_data` function. This function handles the retrieval and transformation of time series data from the current data source.

2. **Customize the Data Retrieval Logic**:
   If your time series source is an API, you will likely need to:
   - Send HTTP requests to the API using a library like `requests`.
   - Parse the returned JSON data.
   - Convert the parsed data into a pandas DataFrame with the appropriate columns (`'date'` and `'price'`).

3. **Ensure Proper Formatting**:
   Make sure that the resulting DataFrame:
   - Contains a `date` column with datetime values.
   - Contains a `price` column with numerical values.
   - Is indexed by the `date` column, ensuring the index is a pandas `DatetimeIndex` with an hourly frequency.

---

## Flexibility of the Time Series Source

The application logic in Forecrypt is designed to analyze any hourly time series using mathematical models. This means that the nature and origin of the time series can be highly flexible. Whether it's cryptocurrency prices, stock market data, or even environmental metrics, as long as your source provides a structured time series, it can be integrated into Forecrypt.

With proper customization of the `fetch_historical_data` function, nearly any time series source should be compatible with the system.
