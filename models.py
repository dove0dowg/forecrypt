import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.forecasting.theta import ThetaModel

def fit_arima_model(df, **kwargs):
    """
    arima model training function.
    all parameters must come from kwargs['specific_parameters'] (e.g. order, seasonal_order).
    """
    order = kwargs['specific_parameters']['order']
    seasonal_order = kwargs['specific_parameters']['seasonal_order']

    model = ARIMA(df['price'], order=order, seasonal_order=seasonal_order)
    model_fit = model.fit()
    return model_fit


def fit_ets_model(df, **kwargs):
    """
    ets model training function.
    all parameters must come from kwargs['specific_parameters'] 
    (e.g., trend, seasonal, seasonal_periods).
    """
    trend = kwargs['specific_parameters']['trend']
    seasonal = kwargs['specific_parameters']['seasonal']
    seasonal_periods = kwargs['specific_parameters']['seasonal_periods']

    model = ExponentialSmoothing(
        df['price'],
        trend=trend,
        seasonal=seasonal,
        seasonal_periods=seasonal_periods
    )
    model_fit = model.fit()
    return model_fit


def fit_theta_model(df, **kwargs):
    """
    theta model training function.
    parameters come from kwargs['specific_parameters'] if needed (like m, method, etc.).
    """
    if 'date' in df.columns:
        df = df.set_index('date')
    elif df.index.name != 'date' and not df.index._is_all_dates:
        raise KeyError("'date' column not found and index is not datetime.")

    df.index = pd.DatetimeIndex(df.index).to_period('h')

    # if you have something like m=24 or method='something' in specific_parameters:
    # e.g.: m = kwargs['specific_parameters']['m']
    # model = ThetaModel(df['price'], m=m)
    # below is a minimal version:
    model = ThetaModel(df['price'])
    model_fit = model.fit()
    return model_fit