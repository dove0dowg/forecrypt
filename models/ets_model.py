from statsmodels.tsa.holtwinters import ExponentialSmoothing

def fit_ets_model(df):
    model = ExponentialSmoothing(df['price'], trend='add', seasonal='mul', seasonal_periods=24)
    model_fit = model.fit()
    return model_fit