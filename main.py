# libs
import psycopg2
import pandas as pd
import importlib
import logging
from datetime import datetime, timezone, timedelta
# modules
import db_utils
import models_processing
from get_data import fetch_specific_historical_hours, fetch_historical_data
from config import CRYPTO_LIST, START_DATE, DB_CONFIG, MODEL_PARAMETERS
from forecasting import create_forecast_dataframe

# ---------------------------------------------------------
# 1. combined and helper functions
# ---------------------------------------------------------

def check_and_save_models_cycle(crypto_list=None, model_names=None):
    """
    processes and saves models for the specified cryptocurrencies and models.
    if no arguments are provided, uses all CRYPTO_LIST and MODEL_PARAMETERS.
    """
    crypto_list = crypto_list or CRYPTO_LIST
    model_names = model_names or MODEL_PARAMETERS

    for crypto_id in crypto_list:
        for model_name in model_names:
            if models_processing.check_model(crypto_id, model_name):
                # fetch historical data
                df = fetch_historical_data(
                    crypto_id, 
                    MODEL_PARAMETERS[model_name]["dataset_hours"]
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

def combine_historical_and_forecast(historical_df, forecast_df):
    """
    combines historical and forecast data into a single dataframe.
    
    :param historical_df: dataframe with historical data, must contain 'date' and 'price' columns.
    :param forecast_df: dataframe with forecast data, must contain 'date' and 'price' columns.
    :return: combined dataframe with historical and forecast data.
    """
    # validate input
    if not {'date', 'price'}.issubset(historical_df.columns):
        raise ValueError("historical_df must contain 'date' and 'price' columns.")
    if not {'date', 'price'}.issubset(forecast_df.columns):
        raise ValueError("forecast_df must contain 'date' and 'price' columns.")

    # ensure 'date' is datetime
    historical_df['date'] = pd.to_datetime(historical_df['date'])
    forecast_df['date'] = pd.to_datetime(forecast_df['date'])

    # concatenate the two dataframes
    combined_df = pd.concat([historical_df, forecast_df], ignore_index=True)

    # sort by date for consistency
    combined_df = combined_df.sort_values(by='date').reset_index(drop=True)

    return combined_df

def test_forecasts():
    """
    generates forecasts for all cryptocurrencies and models,
    and compares forecast formats with historical data.
    """
    for crypto_id in CRYPTO_LIST:
        for model_name in MODEL_PARAMETERS:
            try:
                # load historical data
                historical_df = fetch_historical_data(
                    crypto_id, 
                    MODEL_PARAMETERS[model_name]["dataset_hours"]
                )

                # load trained model
                model_fit = models_processing.load_model(crypto_id, model_name)

                # generate forecast
                forecast_steps = MODEL_PARAMETERS[model_name].get('forecast_steps', 30)
                forecast_df = create_forecast_dataframe(historical_df, model_fit, steps=forecast_steps)

                combined_df = combine_historical_and_forecast(historical_df, forecast_df)

                # print results
                print(f"forecast for {crypto_id} - {model_name}:\n{forecast_df.head()}\n")
                print(f"historical data sample:\n{historical_df.head()}\n")
                print(f"forecast format matches historical format: {list(forecast_df.columns) == list(historical_df.columns)}\n")
                print(combined_df.iloc[710:730])
                print(combined_df)
            except Exception as e:
                print(f"error processing {crypto_id} - {model_name}: {e}")

def get_max_dataset_hours(model_parameters):
    """
    find the maximum dataset_hours among all models.
    """
    return max(model["dataset_hours"] for model in model_parameters.values())

def get_max_historical_df(crypto_id, max_hours):
    """
    get historical data for the maximum required range (max_hours).
    """
    return fetch_historical_data(crypto_id, max_hours)

def extract_model_specific_df(dataframe, dataset_hours):
    """
    extract the last `dataset_hours` rows from the given DataFrame.
    """
    return dataframe.iloc[-dataset_hours:]

def get_max_dataset_hours(model_parameters):
    """find the maximum dataset_hours among all models."""
    return max(model["dataset_hours"] for model in model_parameters.values())

def get_additional_start_date(start_date: datetime, max_hours: int) -> datetime:
    """
    compute the earliest date we need to fetch,
    so that at START_DATE we already have 'max_hours' worth of data.
    """
    return start_date - timedelta(hours=max_hours)

# ---------------------------------------------------------
# 2. configure logging
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
# 3. main logic
# ---------------------------------------------------------

if __name__ == "__main__":
    # connect to DB
    try:
        conn = psycopg2.connect(**db_utils.DB_CONFIG)
        conn.autocommit = True
        logger.info("database connection established.")
    except psycopg2.Error as e:
        logger.error(f"failed to connect to the database: {e}")
        exit()

    try:
        # initialize database tables
        db_utils.create_tables()
        logger.info("database tables initialized.")

        # main time interval (naive datetimes)
        now_naive = datetime.now()  # no tzinfo
        # parse START_DATE as naive datetime
        # если в START_DATE внутри конфиге нет "+00:00", то это сделает наивную дату
        start_naive = pd.to_datetime(START_DATE)
        # если всё же там есть таймзона, уберём её:
        if start_naive.tz is not None:
            start_naive = start_naive.tz_localize(None)

        logger.info(f"START_DATE: {start_naive}, now: {now_naive}")

        # find max hours (to have enough history for any model)
        max_hours = get_max_dataset_hours(MODEL_PARAMETERS)
        logger.debug(f"max dataset_hours: {max_hours}")

        # compute the earliest date we need to fetch
        extended_start_dt = start_naive - timedelta(hours=max_hours)
        logger.debug(f"extended start date: {extended_start_dt}")

        # process each cryptocurrency
        for crypto_id in CRYPTO_LIST:
            logger.info(f"processing crypto: {crypto_id}")

            # fetch historical data (extended range)
            total_hours = int((now_naive - extended_start_dt).total_seconds() // 3600) + 1
            df_extended = fetch_historical_data(crypto_id, total_hours)

            if df_extended.empty:
                logger.error(f"no historical data fetched for {crypto_id}")
                continue

            # ensure df_extended['date'] is naive datetime64[ns]
            df_extended['date'] = pd.to_datetime(df_extended['date'])
            # если там вдруг указана таймзона, убираем
            if pd.api.types.is_datetime64tz_dtype(df_extended['date'].dtype):
                df_extended['date'] = df_extended['date'].dt.tz_localize(None)

            logger.debug(
                f"{crypto_id}: extended df from {df_extended['date'].min()} "
                f"to {df_extended['date'].max()} (rows={len(df_extended)})"
            )

            # define the timeline: hour by hour from start_naive to now_naive
            current_dt = start_naive

            # track last retrain time & last forecast time for each model
            model_last_retrain = {}
            model_last_forecast = {}
            for model_name in MODEL_PARAMETERS.keys():
                model_last_retrain[model_name] = None
                model_last_forecast[model_name] = None

            # go hour by hour
            while current_dt <= now_naive:
                for model_name, params in MODEL_PARAMETERS.items():
                    update_interval = params.get('model_update_interval', 24)
                    forecast_freq = params.get('forecast_frequency', 12)
                    ds_hours = params['dataset_hours']
                    forecast_steps = params.get('forecast_steps', 30)

                    earliest_needed_dt = current_dt - timedelta(hours=ds_hours)

                    # фильтрация по наивным датам
                    sub_df = df_extended[
                        (df_extended['date'] >= earliest_needed_dt) &
                        (df_extended['date'] <= current_dt)
                    ]

                    if len(sub_df) < ds_hours:
                        continue

                    # check retrain
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
                            model_fit = models_processing.fit_model_any(sub_df, model_name)
                            models_processing.save_model(crypto_id, model_name, model_fit)
                            model_last_retrain[model_name] = current_dt
                            logger.debug(f"[{crypto_id} - {model_name}] Model retrained and saved.")
                        except Exception as e:
                            logger.error(f"[{crypto_id} - {model_name}] Error during retraining: {e}")
                    else:
                        try:
                            logger.debug(f"[{crypto_id} - {model_name}] Loading existing model.")
                            model_fit = models_processing.load_model(crypto_id, model_name)
                            logger.debug(f"[{crypto_id} - {model_name}] Model loaded successfully.")
                        except Exception as e:
                            logger.error(f"[{crypto_id} - {model_name}] Error loading model: {e}")
                            continue

                    # check forecast
                    do_forecast = False
                    if model_last_forecast[model_name] is None:
                        do_forecast = True
                    else:
                        hours_since_forecast = (current_dt - model_last_forecast[model_name]).total_seconds() / 3600
                        if hours_since_forecast >= forecast_freq:
                            do_forecast = True

                    if do_forecast:
                        df_forecast = create_forecast_dataframe(sub_df, model_fit, steps=forecast_steps)
                        db_utils.load_to_db_forecast(df_forecast, crypto_id, model_name, conn, created_at=current_dt)
                        logger.info(f"[{crypto_id} - {model_name}] forecast saved at {current_dt}")
                        model_last_forecast[model_name] = current_dt

                current_dt += timedelta(hours=1)

        logger.info(f"model processing completed at {datetime.now()}")

    except Exception as e:
        logger.critical(f"an error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("database connection closed.")