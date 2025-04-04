import pandas as pd

def create_forecast_dataframe(df, model_fit, steps):
    """
    generate a pandas DataFrame containing a forecast based on the input DataFrame and model.

    :param df: Historical cryptocurrency data as a pandas DataFrame with columns ['date', 'price'].
    :param model_fit: A trained forecasting model.
    :param steps: Number of forecast steps (hours) to generate.
    :return: A pandas DataFrame with columns ['date', 'price'] containing the forecast data.
    """
    # ensure input is a DataFrame and validate structure
    if not isinstance(df, pd.DataFrame):
        raise ValueError("The input must be a DataFrame.")
    if not {'date', 'price'}.issubset(df.columns):
        raise ValueError("The DataFrame must contain 'date' and 'price' columns.")

    # set 'date' as index and ensure it's a DatetimeIndex
    df = df.set_index('date')
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # extract 'price' column
    price_series = df['price']

    # generate forecast
    if hasattr(model_fit, 'get_forecast'):  # for ARIMA models
        forecast_result = model_fit.get_forecast(steps=steps)
        forecast = forecast_result.predicted_mean
    else:  # for ETS, Theta models
        forecast = model_fit.forecast(steps=steps)

    # generate timestamps for forecast (steps timestamps starting from the next hour)
    forecast_dates = pd.date_range(
        start=price_series.index[-1] + pd.Timedelta(hours=1),  # start after the last historical point
        periods=steps,  # generate exactly 'steps' timestamps
        freq='h'
    )
    
    # create forecast series with timestamps
    forecast_series = pd.Series(forecast.values, index=forecast_dates)
    
    # add zero step (last historical value) manually
    zero_step = pd.Series([price_series.iloc[-1]], index=[price_series.index[-1]])
    
    # concatenate zero step with forecast
    forecast_series = pd.concat([zero_step, forecast_series])
    
    # convert to DataFrame
    forecast_df = forecast_series.reset_index()
    forecast_df.columns = ['date', 'price']  # match historical data
    
    return forecast_df
