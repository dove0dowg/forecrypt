import pandas as pd
import json

def create_forecast_json(crypto_df, model_fit, steps=31): 
    """
    Generate a JSON string containing a forecast based on the input DataFrame and model.
    """
    # ensure input is a DataFrame and extract 'price' column
    if isinstance(crypto_df, pd.DataFrame):
        if 'price' not in crypto_df.columns:
            raise ValueError("The DataFrame does not contain a 'price' column.")
        crypto_df = crypto_df['price']

    # ensure DataFrame index is a datetime index
    if isinstance(crypto_df.index, pd.PeriodIndex):
        crypto_df.index = crypto_df.index.to_timestamp()

    # generate forecast
    if hasattr(model_fit, 'get_forecast'):  # ARIMA models
        forecast_result = model_fit.get_forecast(steps=steps - 1)
        forecast = forecast_result.predicted_mean
    else:  # ETS, Theta models
        forecast = model_fit.forecast(steps=steps - 1)
    
    # generate timestamps for forecast
    forecast_dates = pd.date_range(
        start=crypto_df.index[-1] + pd.Timedelta(hours=1), 
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
