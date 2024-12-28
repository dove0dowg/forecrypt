import pandas as pd

def create_forecast_dataframe(crypto_df, model_fit, steps):
    """
    generate a pandas DataFrame containing a forecast based on the input DataFrame and model.
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
    if hasattr(model_fit, 'get_forecast'):  # for ARIMA models
        forecast_result = model_fit.get_forecast(steps=steps)
        forecast = forecast_result.predicted_mean
    else:  # for ETS, Theta models
        forecast = model_fit.forecast(steps=steps)

    # generate timestamps for forecast
    forecast_dates = pd.date_range(
        start=price_series.index[-1] + pd.Timedelta(hours=1),  # start after the last historical point
        periods=steps,  # generate exactly 'steps' timestamps
        freq='h'
    )
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # convert to DataFrame
    forecast_df = forecast_series.reset_index()
    forecast_df.columns = ['date', 'price']  # match historical data

    return forecast_df
