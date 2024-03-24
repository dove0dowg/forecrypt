from statsmodels.tsa.arima.model import ARIMA

def fit_arima_model(df, order=(24,3,8)): 
    model = ARIMA(df['price'], order=order)
    model_fit = model.fit()
    return model_fit
