from statsmodels.tsa.forecasting.theta import ThetaModel
import pandas as pd

def fit_theta_model(df):
    # Only set 'date' as index if it exists and is not already the index
    if 'date' in df.columns:
        df = df.set_index('date')
    elif df.index.name != 'date' and not df.index._is_all_dates:
        raise KeyError("'date' column not found and index is not datetime.")
    df.index = pd.DatetimeIndex(df.index).to_period('h')  # Convert to period index with hourly frequency
    
    model = ThetaModel(df['price'])
    model_fit = model.fit()
    return model_fit