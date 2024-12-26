import pandas as pd
import json

def create_forecast_json(crypto_df, model_fit, steps=31): 
    """
    Generate a JSON string containing a forecast based on the input DataFrame and model.
    """
    # ensure input is a DataFrame and validate structure
    if not isinstance(crypto_df, pd.DataFrame):
        raise ValueError("The input must be a DataFrame.")
    if not {'date', 'price'}.issubset(crypto_df.columns):
        raise ValueError("The DataFrame must contain 'date' and 'price' columns.")

    # set 'date' as index and ensure it's a DatetimeIndex
    crypto_df = crypto_df.set_index('date')
    if not isinstance(crypto_df.index, pd.DatetimeIndex):
        crypto_df.index = pd.to_datetime(crypto_df.index)

    # extract 'price' column
    price_series = crypto_df['price']

    # generate forecast
    if hasattr(model_fit, 'get_forecast'):  # ARIMA models
        forecast_result = model_fit.get_forecast(steps=steps - 1)
        forecast = forecast_result.predicted_mean
    else:  # ETS, Theta models
        forecast = model_fit.forecast(steps=steps - 1)

    # generate timestamps for forecast
    forecast_dates = pd.date_range(
        start=price_series.index[-1] + pd.Timedelta(hours=1), 
        periods=steps - 1, 
        freq='h'
    )
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # convert forecast data to dictionary with ISO 8601 timestamps
    forecast_dict = {
        index.isoformat(): value for index, value in forecast_series.items()
    }

    # convert dictionary to JSON string
    json_str = json.dumps(forecast_dict)

    return json_str