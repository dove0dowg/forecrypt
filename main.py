# ---------------------------------------------------------
# libs
# ---------------------------------------------------------
import pandas as pd
import importlib
import logging
from datetime import datetime, timezone, timedelta
# ---------------------------------------------------------
# modules
# ---------------------------------------------------------
import db_utils
import models_processing
import get_data
from config import CRYPTO_LIST, START_DATE, FINISH_DATE, DB_CONFIG, MODEL_PARAMETERS
from forecasting import create_forecast_dataframe
# ---------------------------------------------------------
# ---------------------------------------------------------
# 1. configure logging
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("forecrypt.log")
    ]
)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------
# 2. combined and helper functions
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
    max_hours = max(params["training_dataset_size"] for params in model_parameters.values())
    extended_start_dt = start_naive - timedelta(hours=max_hours)

    # Calculate total hours to fetch
    total_hours = int((finish_naive - extended_start_dt).total_seconds() // 3600) + 1

    # Logging
    logger.info(
        f"Forecasts will be created for period: START_DATE: {start_naive}, FINISH_DATE: {finish_naive}.\n"
        f"Historical data extended by {max_hours} hours of training dataset.\n"
        f"New start date for extended historical data:{extended_start_dt}.\n"
        f"{total_hours} hours of data will be fetched"
        )

    return start_naive, finish_naive, total_hours, extended_start_dt

def fetch_extended_df(crypto_id: str, total_hours: int):
    """
    Fetch extended historical data for a cryptocurrency through get_data.fetch_historical_data function.

    :param crypto_id: Cryptocurrency identifier (e.g., 'BTC').
    :param total_hours: Total number of hours to fetch.
    :return: DataFrame with historical data or None if data is empty.
    """
    extended_df = get_data.fetch_historical_data(crypto_id, total_hours)

    if extended_df.empty:
        logger.error(f"No historical data fetched for {crypto_id}")
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
    train_df = extended_df[
        (extended_df["date"] >= current_dt - timedelta(hours=training_dataset_size)) &
        (extended_df["date"] <= current_dt)
    ]

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

# ---------------------------------------------------------
# 3. main logic
# ---------------------------------------------------------
if __name__ == "__main__":

    # Connect to DB

    conn = db_utils.init_database_connection()

    try:
        # Initialize database tables
        db_utils.create_tables(conn)
        
        db_utils.create_combined_view(conn)

        # Calculate fetch intervals
        # start_naive
        # finish_naive
        # total_hours
        # 
        start_naive, finish_naive, total_hours, extended_start_dt = calculate_total_fetch_interval(
            START_DATE, finish_date=FINISH_DATE, **MODEL_PARAMETERS
        )

        # Cycle for each cryptocurrency
        for crypto_id in CRYPTO_LIST:
            logger.info(f"Processing cryptocurrency: {crypto_id}")

            # Fetch extended historical dataset (max hours for training dataset + historical dataset from START_DATE to FINISH_DATE)
            extended_df = fetch_extended_df(crypto_id, total_hours)

            # Load historical data into the database
            db_utils.load_to_db_historical(extended_df, crypto_id, conn)

            # Initialize tracking for retrain and forecast. 
            model_last_retrain, model_last_forecast = initialize_model_tracking()

            # Cycle for each model
            for model_name, params in MODEL_PARAMETERS.items():
                logger.info(f"Processing model: {model_name} at {datetime.now()}")

                # Process hourly data
                current_dt = start_naive
                while current_dt <= finish_naive:    

                    # Get training dataset
                    training_dataset_size = params["training_dataset_size"]
                    train_df = get_train_df(extended_df, current_dt, training_dataset_size, crypto_id, model_name)
                    
                    # Get forecast input dataset
                    forecast_dataset_size = params["forecast_dataset_size"]
                    forecast_input_df = get_forecast_input_df(extended_df, current_dt, forecast_dataset_size, crypto_id, model_name)
                    
                    # Handle retraining. Check if retrain required.
                    # If required - retrain and save model.
                    models_processing.retrain_in_hour_cycle(
                        model_name=model_name,
                        params=params,
                        sub_df=train_df,
                        current_dt=current_dt,
                        crypto_id=crypto_id,
                        model_last_retrain=model_last_retrain,
                    )

                    # Handle forecasting. Checking config.py if forecast required.
                    # If yes - load model, create forecast, and load to db.
                    # If no - skip.
                    models_processing.forecast_in_hour_cycle(
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