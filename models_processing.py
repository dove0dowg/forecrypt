import os
import joblib
import logging
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