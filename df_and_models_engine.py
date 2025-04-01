
# libs
import pandas as pd
import importlib
import logging
from datetime import datetime, timezone, timedelta
# modules
import db_utils_postgres
import models_processing
import get_data
from config import CRYPTO_LIST, START_DATE, FINISH_DATE, PG_DB_CONFIG, CH_DB_CONFIG, MODEL_PARAMETERS
# ---------------------------------------------------------
# [Logger]
# ---------------------------------------------------------
logger = logging.getLogger(__name__)
# ---------------------------------------------------------
# [DF and models processing] ()
# ---------------------------------------------------------
def check_and_save_models_cycle(crypto_list=None, model_names=None):
    """
    processes and saves models for the specified cryptocurrencies and models.
    if no arguments are provided, processes all cryptocurrencies in CRYPTO_LIST and models in MODEL_PARAMETERS.
    
    automatically retrains models based on the configured update interval and saves forecasts.
    """
    crypto_list = crypto_list or CRYPTO_LIST
    model_names = model_names or MODEL_PARAMETERS

    for crypto_id in crypto_list:
        for model_name in model_names:
            if models_processing.check_model(crypto_id, model_name):
                # fetch historical data
                df = get_data.fetch_historical_data(
                    crypto_id, 
                    MODEL_PARAMETERS[model_name]["training_dataset_size"]
                )

                # dynamically import the fit function
                fit_func_path = MODEL_PARAMETERS[model_name]["fit_func_name"]
                module_name, func_name = fit_func_path.rsplit(".", 1)
                module = importlib.import_module(module_name)
                fit_func = getattr(module, func_name)

                # train the model
                model_fit = fit_func(df, **MODEL_PARAMETERS[model_name])

                # save the model
                models_processing.save_model(crypto_id, model_name, model_fit)

def calculate_total_fetch_interval(start_date: str, finish_date: str = None, **model_parameters):
    """
    Calculate the time intervals and total hours needed to fetch historical data.

    :param start_date: Start date string from configuration.
    :param finish_date: Optional finish date string from configuration. Defaults to the current time if not provided.
    :param model_parameters: Named model parameters (e.g., arima, ets).
    :return: Tuple (start_naive, finish_naive, total_hours, extended_start_dt).
    """
    start_naive = pd.to_datetime(start_date)

    # Ensure start_naive is naive
    if start_naive.tz is not None:
        start_naive = start_naive.tz_localize(None)

    # Determine finish_naive
    if finish_date is None or finish_date.lower() == "now":
        finish_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        logger.info("FINISH_DATE is set to NOW (current time).")
    else:
        finish_naive = pd.to_datetime(finish_date)
        if finish_naive.tz is not None:
            finish_naive = finish_naive.tz_localize(None)
        logger.info(f"FINISH_DATE is set to {finish_naive}.")

    # Calculate maximum hours required
    max_train_dataset_hours = max(params["training_dataset_size"] for params in model_parameters.values())
    extended_start_dt = start_naive - timedelta(hours=max_train_dataset_hours)

    # Calculate total hours to fetch
    total_hours = int((finish_naive - extended_start_dt).total_seconds() // 3600) + 1

    # Logging
    logger.info(
        f"Forecasts will be created for period: START_DATE: {start_naive}, FINISH_DATE: {finish_naive}.\n"
        f"Historical data now contains {total_hours} hours, extended by {max_train_dataset_hours} hours of training dataset.\n"
        f"New start date for extended historical data: {extended_start_dt}."
        )

    return start_naive, finish_naive, total_hours, extended_start_dt, max_train_dataset_hours

def fetch_extended_df(crypto_id: str, start_date, end_date, api_key=None):
    """
    Fetch extended historical data for a cryptocurrency through get_data.fetch_historical_data function.

    :param crypto_id: Cryptocurrency identifier (e.g., 'BTC').
    :param total_hours: Total number of hours to fetch.
    :return: DataFrame with historical data or None if data is empty.
    """
    
    extended_df = get_data.fetch_historical_data(crypto_id, start_date, end_date, api_key)
    logger.debug(f"5 debug df call: {extended_df.head()}")

    if extended_df.empty:
        logger.critical(f"No historical data fetched for {crypto_id}")
        return None

    logger.debug(
        f"{crypto_id}: extended df from {extended_df['date'].min()} "
        f"to {extended_df['date'].max()} (rows={len(extended_df)})"
    )
    return extended_df

def get_train_df(extended_df, current_dt, training_dataset_size, crypto_id, model_name):
    """
    Extracts the training dataset for a specific time window.

    Args:
        extended_df (DataFrame): Full historical dataset.
        current_dt (datetime): Current datetime being processed.
        training_dataset_size (int): Number of hours required for training.
        crypto_id (str): Cryptocurrency ID.
        model_name (str): Name of the model.

    Returns:
        DataFrame: Training dataset for the model, or None if data is insufficient.
    """
    logger.debug(f"6 debug df call: {extended_df.head()}")

    train_df = extended_df[
        (extended_df["date"] >= current_dt - timedelta(hours=training_dataset_size)) &
        (extended_df["date"] <= current_dt)
    ]
    
    logger.debug(f"7 debug df call: {train_df.head()}")
    
    if len(train_df) < training_dataset_size:
        logger.debug(f"Not enough training data for {crypto_id} - {model_name} at {current_dt}, skipping.")
        return None

    return train_df

def get_forecast_input_df(extended_df, current_dt, forecast_dataset_size, crypto_id, model_name):
    """
    Extracts the forecast input dataset for a specific time window.

    Args:
        extended_df (DataFrame): Full historical dataset.
        current_dt (datetime): Current datetime being processed.
        forecast_dataset_size (int): Number of hours required for forecasting.
        crypto_id (str): Cryptocurrency ID.
        model_name (str): Name of the model.

    Returns:
        DataFrame: Forecast input dataset for the model, or None if data is insufficient.
    """
    forecast_input_df = extended_df[
        (extended_df["date"] >= current_dt - timedelta(hours=forecast_dataset_size)) &
        (extended_df["date"] <= current_dt)
    ]

    if len(forecast_input_df) < forecast_dataset_size:
        logger.debug(f"Not enough forecast input data for {crypto_id} - {model_name} at {current_dt}, skipping.")
        return None

    return forecast_input_df

def initialize_model_tracking():
    """
    Initialize tracking dictionaries for retraining and forecasting.
    
    Returns:
        Tuple[dict, dict]: Dictionaries for model_last_retrain and model_last_forecast.
    """
    model_last_retrain = {model: None for model in MODEL_PARAMETERS.keys()}
    model_last_forecast = {model: None for model in MODEL_PARAMETERS.keys()}
    
    return model_last_retrain, model_last_forecast

def retrain_in_hour_cycle(*, model_name, params, sub_df, current_dt, crypto_id, model_last_retrain):
    """
    Handle model retraining for a specific hour.

    This function determines whether the model requires retraining based on the elapsed time
    since the last retraining. If retraining is necessary, the model is trained using the provided
    historical data and saved. If retraining is not required, no action is taken.

    Args:
        model_name (str): The name of the model being processed. This is used for logging and 
                          identifying the model in the database.
        params (dict): A dictionary containing configuration parameters for the model. 
                       Must include 'model_update_interval', which defines the time interval 
                       (in hours) for retraining.
        sub_df (DataFrame): A pandas DataFrame containing historical data used for training the model.
        current_dt (datetime): The current datetime being processed in the hourly cycle.
        crypto_id (str): The cryptocurrency identifier (e.g., 'BTC', 'ETH') for which the model is being processed.
        model_last_retrain (dict): A dictionary tracking the last retrain times for each model. 
                                   The key is the model name, and the value is the datetime of the last retraining.

    Returns:
        None

    Raises:
        Exception: Logs any exceptions that occur during model fitting or saving.
    """
    update_interval = params.get('model_update_interval')

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

    if do_retrain:
        try:
            model_fit = models_processing.fit_model_any(sub_df, model_name)
            models_processing.save_model(crypto_id, model_name, model_fit)
            model_last_retrain[model_name] = current_dt
            logger.debug(f"[{crypto_id} - {model_name}] Model retrained and saved.")
        except Exception as e:
            logger.error(f"[{crypto_id} - {model_name}] Error during retraining: {e}")

def forecast_in_hour_cycle(*, model_name, params, sub_df, current_dt, crypto_id, conn, model_last_forecast):
    """
    Handle forecasting for a specific hour.

    This function checks whether a new forecast is needed based on the elapsed time since the
    last forecast. If a forecast is required, the function attempts to load the existing model,
    generates predictions for the specified number of hours, and saves the forecast results to the database.

    Args:
        model_name (str): The name of the model being used for forecasting.
        params (dict): A dictionary containing configuration parameters for the model. 
                       Must include 'forecast_frequency' (time interval in hours between forecasts) 
                       and 'forecast_hours' (number of hours to forecast).
        sub_df (DataFrame): A pandas DataFrame containing the input data for forecasting.
        current_dt (datetime): The current datetime being processed in the hourly cycle.
        crypto_id (str): The cryptocurrency identifier (e.g., 'BTC', 'ETH') for which the forecast is being generated.
        conn (Connection): A database connection object used to save the forecast results.
        model_last_forecast (dict): A dictionary tracking the last forecast times for each model. 
                                    The key is the model name, and the value is the datetime of the last forecast.

    Returns:
        None

    Raises:
        Exception: Logs any exceptions that occur during forecasting or saving the forecast.
    """
    forecast_freq = params.get('forecast_frequency')
    forecast_hours = params.get('forecast_hours')

    do_forecast = False
    if model_last_forecast[model_name] is None:
        do_forecast = True
    else:
        hours_since_forecast = (current_dt - model_last_forecast[model_name]).total_seconds() / 3600
        if hours_since_forecast >= forecast_freq:
            do_forecast = True

    if do_forecast:
        try:
            logger.debug(f"[{crypto_id} - {model_name}] Loading existing model for forecasting.")
            model_fit = models_processing.load_model(crypto_id, model_name)
            logger.debug(f"[{crypto_id} - {model_name}] Model loaded successfully.")

            df_forecast = models_processing.create_forecast_dataframe(sub_df, model_fit, steps=forecast_hours)
            models_processing.load_to_db_forecast(df_forecast, crypto_id, model_name, params, conn, created_at=current_dt)
            logger.debug(f"[{crypto_id} - {model_name}] Forecast saved for {current_dt}.")
            model_last_forecast[model_name] = current_dt
        except Exception as e:
            logger.error(f"[{crypto_id} - {model_name}] Error during forecasting: {e}")
# ---------------------------------------------------------
# [Final function] (to call from main)
def fetch_predict_upload_ts(conn):

    try:
        # Calculate fetch intervals
        start_naive, finish_naive, total_hours, extended_start_dt, max_train_dataset_hours = calculate_total_fetch_interval(
            START_DATE, FINISH_DATE, **MODEL_PARAMETERS
        )

        # Cycle for each cryptocurrency
        for crypto_id in CRYPTO_LIST:
            logger.info(f"Processing cryptocurrency: {crypto_id}")

            # Initialize tracking for retrain and forecast. 
            model_last_retrain, model_last_forecast = initialize_model_tracking()

            # Fetch extended historical dataset (max hours for training dataset + historical dataset from START_DATE to FINISH_DATE)
            extended_df = fetch_extended_df(crypto_id, extended_start_dt, FINISH_DATE)

            # Load historical data into the database
            db_utils_postgres.load_to_db_train_and_historical(extended_df, crypto_id, conn, max_train_dataset_hours)

            # Cycle for each model
            for model_name, params in MODEL_PARAMETERS.items():
                logger.debug(f"Processing model: {model_name} at {datetime.now()}")

                # Process hourly data
                current_dt = start_naive
                while current_dt <= finish_naive:    

                    # Get training dataset
                    training_dataset_size = params["training_dataset_size"]
                    train_df = get_train_df(extended_df, current_dt, training_dataset_size, crypto_id, model_name)
                    
                    logger.debug(train_df.head())
                    logger.debug(f"Index type: {type(train_df.index)}, Is DatetimeIndex: {isinstance(train_df.index, pd.DatetimeIndex)}")
                    logger.debug(f"Missing values: {train_df.isnull().sum()}")

                    # Get forecast input datasetl
                    forecast_dataset_size = params["forecast_dataset_size"]
                    forecast_input_df = get_forecast_input_df(extended_df, current_dt, forecast_dataset_size, crypto_id, model_name)
                    
                    # Handle retraining. Check if retrain required.
                    # If required - retrain and save model.
                    # Dictionary model_last_retrain affected by this function.
                    retrain_in_hour_cycle(
                        model_name=model_name,
                        params=params,
                        sub_df=train_df,
                        current_dt=current_dt,
                        crypto_id=crypto_id,
                        model_last_retrain=model_last_retrain,
                    )

                    # Handle forecasting. Checking config.py if forecast required at this timestamp.
                    # If yes - load model, create forecast, and load to db.
                    # If no - skip.
                    # Dictionary model_last_forecast affected by this function.
                    forecast_in_hour_cycle(
                        model_name=model_name,
                        params=params,
                        sub_df=forecast_input_df,
                        current_dt=current_dt,
                        crypto_id=crypto_id,
                        conn=conn,
                        model_last_forecast=model_last_forecast,
                    )

                    # continue to next hour
                    current_dt += timedelta(hours=1)

                logger.info(f"{model_name} model processing completed successfully at {datetime.now()}")

    except Exception as e:
        logger.critical(f"An error occurred: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")