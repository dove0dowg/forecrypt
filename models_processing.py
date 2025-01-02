import os
import joblib
import logging
from db_utils import load_to_db_forecast
from forecasting import create_forecast_dataframe
from datetime import datetime, timezone
import importlib
from config import MODELS_DIRECTORY, MODEL_PARAMETERS

# initialize logger
logger = logging.getLogger(__name__)

def check_model(crypto_id: str, model_name: str) -> bool:
    """
    Checks if a model exists and determines if it needs to be updated.
    
    :param crypto_id: e.g. 'BTC'
    :param model_name: e.g. 'arima'
    :return: True if model needs update or doesn't exist, False otherwise.
    """
    update_interval_hours = MODEL_PARAMETERS[model_name].get("model_update_interval", 24)

    # construct the file path
    filename = f"{crypto_id}__{model_name}.pkl"
    filepath = os.path.join(MODELS_DIRECTORY, filename)

    if not os.path.exists(filepath):
        logger.debug(f"Model '{filepath}' does not exist. Needs to be created.")
        return True

    # check the last modified time
    mtime = os.path.getmtime(filepath)
    last_modified_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)
    diff_hours = (now_dt - last_modified_dt).total_seconds() / 3600

    if diff_hours >= update_interval_hours:
        logger.debug(
            f"Model '{filepath}' is outdated ({diff_hours:.2f}h old, "
            f"threshold is {update_interval_hours}h). Needs update."
        )
        return True

    logger.debug(
        f"Model '{filepath}' is up-to-date ({diff_hours:.2f}h old, "
        f"threshold is {update_interval_hours}h). No update required."
    )
    return False

def save_model(crypto_id: str, model_name: str, model_fit):
    """
    Saves the given model to a file.
    
    :param crypto_id: e.g. 'BTC'
    :param model_name: e.g. 'arima'
    :param model_fit: fitted model object
    """
    os.makedirs(MODELS_DIRECTORY, exist_ok=True)

    # construct the file path
    filename = f"{crypto_id}__{model_name}.pkl"
    filepath = os.path.join(MODELS_DIRECTORY, filename)

    # save the model
    joblib.dump(model_fit, filepath)
    logger.debug(f"Model saved at '{filepath}'")

def load_model(crypto_id: str, model_name: str):
    """
    Loads the saved model for the given cryptocurrency and model name.
    
    :param crypto_id: e.g., 'BTC'
    :param model_name: e.g., 'arima'
    :return: the deserialized model object
    :raises FileNotFoundError: if the model file does not exist
    """
    # construct the file path
    filename = f"{crypto_id}__{model_name}.pkl"
    filepath = os.path.join(MODELS_DIRECTORY, filename)

    # check if the file exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Model file '{filepath}' does not exist.")

    # load and return the model
    model_fit = joblib.load(filepath)
    logger.debug(f"Model loaded from '{filepath}'")
    return model_fit

def fit_model_any(df, model_name):
    """
    Dynamically fit any model based on model_name.
    """
    if model_name not in MODEL_PARAMETERS:
        raise ValueError(f"model_name '{model_name}' not found in MODEL_PARAMETERS")

    # get path to the fitting function
    fit_func_path = MODEL_PARAMETERS[model_name]["fit_func_name"]
    module_name, func_name = fit_func_path.rsplit(".", 1)

    # dynamically import
    module = importlib.import_module(module_name)
    fit_func = getattr(module, func_name)

    logger.debug(f"Dynamically importing function {func_name} from module {module_name}")
    
    # call the fitting function
    logger.debug(f"Calling {fit_func} for {model_name} with parameters: {MODEL_PARAMETERS[model_name]}")
    model_fit = fit_func(df, **MODEL_PARAMETERS[model_name])
    
    logger.debug(f"Model {model_name} fitted successfully: {model_fit}")
    return model_fit

def process_model_for_hour(*, model_name, params, sub_df, current_dt, crypto_id, conn, model_last_retrain, model_last_forecast):
    """
    Process a single model for a specific cryptocurrency and time interval.
    This function handles retraining or loading an existing model, checking whether a forecast is needed,
    and generating and saving the forecast.
    Args:
        model_name (str): Name of the model to process.
        params (dict): Configuration parameters for the model, including update interval and forecast frequency.
        sub_df (DataFrame): Filtered historical data for the specific time window.
        current_dt (datetime): The current datetime being processed.
        crypto_id (str): Identifier for the cryptocurrency (e.g., 'BTC', 'ETH').
        conn (Connection): Database connection for saving forecasts.
        model_last_retrain (dict): Tracks the last retrain time for each model.
        model_last_forecast (dict): Tracks the last forecast time for each model.
    Returns:
        None
    Logic:
        - Filters the input data to ensure sufficient historical coverage.
        - Checks whether retraining is needed based on the model's update interval.
        - Either retrains the model or loads an existing one.
        - Checks if a forecast is needed based on the model's forecast frequency.
        - Generates a forecast and saves it to the database if required.
    """
    
    update_interval = params.get('model_update_interval')
    forecast_freq = params.get('forecast_frequency')
    forecast_hours = params.get('forecast_hours')
    
    # Check retrain
    do_retrain = False
    if model_last_retrain[model_name] is None:
        logger.debug(f"[{crypto_id} - {model_name}] No previous retrain. Initiating first training.")
        do_retrain = True
    else:
        hours_since_retrain = (current_dt - model_last_retrain[model_name]).total_seconds() / 3600
        logger.debug(f"[{crypto_id} - {model_name}] Hours since last retrain: {hours_since_retrain}, update interval: {update_interval}.")
        if hours_since_retrain >= update_interval:
            logger.debug(f"[{crypto_id} - {model_name}] Update interval exceeded. Marking for retraining.")
            do_retrain = True
        else:
            logger.debug(f"[{crypto_id} - {model_name}] Update interval not exceeded. Skipping retrain.")
    
    if do_retrain:
        try:
            logger.debug(f"[{crypto_id} - {model_name}] Retraining model at {current_dt}.")
            model_fit = fit_model_any(sub_df, model_name)
            save_model(crypto_id, model_name, model_fit)
            model_last_retrain[model_name] = current_dt
            logger.debug(f"[{crypto_id} - {model_name}] Model retrained and saved.")
        except Exception as e:
            logger.error(f"[{crypto_id} - {model_name}] Error during retraining: {e}")
            return
    else:
        try:
            logger.debug(f"[{crypto_id} - {model_name}] Loading existing model.")
            model_fit = load_model(crypto_id, model_name)
            logger.debug(f"[{crypto_id} - {model_name}] Model loaded successfully.")
        except Exception as e:
            logger.error(f"[{crypto_id} - {model_name}] Error loading model: {e}")
            return
    
    # Check forecast
    do_forecast = False
    if model_last_forecast[model_name] is None:
        do_forecast = True
    else:
        hours_since_forecast = (current_dt - model_last_forecast[model_name]).total_seconds() / 3600
        if hours_since_forecast >= forecast_freq:
            do_forecast = True
    if do_forecast:
        df_forecast = create_forecast_dataframe(sub_df, model_fit, steps=forecast_hours)
        load_to_db_forecast(df_forecast, crypto_id, model_name, conn, created_at=current_dt)
        logger.debug(f"[{crypto_id} - {model_name}] forecast saved for {current_dt}")
        model_last_forecast[model_name] = current_dt