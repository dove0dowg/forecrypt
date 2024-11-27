from models import arima_model as a_m, ets_model as ets_m, theta_model as th_m

model_params = {
    'arima': {'model_flag': "arima", 'dataset_hours': 720, 'balanced_hours': 10, 'trash_hours': 20, 'model_fit_func': a_m.fit_arima_model},
    'ets': {'model_flag': "ets", 'dataset_hours': 720, 'balanced_hours': 15, 'trash_hours': 15, 'model_fit_func': ets_m.fit_ets_model},
    'theta': {'model_flag': "theta", 'dataset_hours': 720, 'balanced_hours': 20, 'trash_hours': 10, 'model_fit_func': th_m.fit_theta_model}
}