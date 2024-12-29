# libs
import psycopg2
import pandas as pd
import importlib
import logging
from datetime import datetime, timezone
# modules
import db_utils
import models_processing
from get_data import fetch_specific_historical_hours, fetch_historical_data
from config import CRYPTO_LIST, START_DATE, DB_CONFIG, MODEL_PARAMETERS
from forecasting import create_forecast_dataframe

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

# basic configuration
logging.basicConfig(
    level=logging.INFO,  # logger level
    format="%(asctime)s [%(levelname)s] %(message)s",  # format
    handlers=[
        logging.StreamHandler(),  # terminal
        logging.FileHandler("forecrypt.log") # file 
    ]
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
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

        # log the start time
        now = datetime.now(timezone.utc)
        logger.info(f"starting model processing at {now.isoformat()}")

        # process each cryptocurrency
        for crypto_id in CRYPTO_LIST:
            logger.info(f"processing {crypto_id}...")

            # determine the maximum dataset_hours
            max_hours = get_max_dataset_hours(MODEL_PARAMETERS)
            logger.info(f"max dataset_hours for {crypto_id}: {max_hours}")

            # get historical data for the maximum range
            max_historical_df = get_max_historical_df(crypto_id, max_hours)
            logger.info(f"fetched max historical data for {crypto_id}, rows: {len(max_historical_df)}")

            # iterate over models
            for model_name, params in MODEL_PARAMETERS.items():
                try:
                    # extract model-specific data
                    model_specific_df = extract_model_specific_df(max_historical_df, params["dataset_hours"])

                    # load model and make forecast
                    model_fit = models_processing.load_model(crypto_id, model_name)
                    forecast_steps = params.get("forecast_steps", 30)
                    df_forecast = create_forecast_dataframe(model_specific_df, model_fit, steps=forecast_steps)

                    # save forecast to the database
                    created_at = model_specific_df["date"].max()
                    db_utils.load_to_db_forecast(df_forecast, crypto_id, model_name, conn, created_at)
                    logger.info(f"forecast for {crypto_id} - {model_name} saved to database.")

                except Exception as e:
                    logger.error(f"error processing {crypto_id} - {model_name}: {e}")

        # log completion
        logger.info(f"model processing completed at {datetime.now(timezone.utc).isoformat()}")

    except Exception as e:
        logger.critical(f"an error occurred: {e}")

    finally:
        if conn:
            conn.close()
            logger.info("database connection closed.")