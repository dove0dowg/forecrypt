import os
import joblib
from datetime import datetime, timezone
from config import MODELS_DIRECTORY, MODEL_PARAMETERS

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
        print(f"Model '{filepath}' does not exist. Needs to be created.")
        return True

    # check the last modified time
    mtime = os.path.getmtime(filepath)
    last_modified_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)
    diff_hours = (now_dt - last_modified_dt).total_seconds() / 3600

    if diff_hours >= update_interval_hours:
        print(
            f"Model '{filepath}' is outdated ({diff_hours:.2f}h old, "
            f"threshold is {update_interval_hours}h). Needs update."
        )
        return True

    print(
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
    print(f"Model saved at '{filepath}'")

def load_model(crypto_id: str, model_name: str):
    """
    loads the saved model for the given cryptocurrency and model name.
    
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
    print(f"Model loaded from '{filepath}'")
    return model_fit