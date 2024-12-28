from datetime import datetime, timezone
import psycopg2
from db_utils import (
    create_tables,
    get_missing_hours,
    load_to_db_historical,
    check_consistency
)
from get_data import fetch_specific_historical_hours, fetch_historical_data
from config import CRYPTO_LIST, START_DATE, DB_CONFIG, CRYPTO_LIST, MODEL_PARAMETERS
from models_processing import check_model, save_model, load_model
import importlib

def check_and_save_models_cycle(crypto_list=None, model_names=None):
    """
    processes and saves models for the specified cryptocurrencies and models.
    if no arguments are provided, uses all CRYPTO_LIST and MODEL_PARAMETERS.
    """
    crypto_list = crypto_list or CRYPTO_LIST
    model_names = model_names or MODEL_PARAMETERS

    for crypto_id in crypto_list:
        for model_name in model_names:
            if check_model(crypto_id, model_name):
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
                save_model(crypto_id, model_name, model_fit)


try:
    db_conn = psycopg2.connect(**DB_CONFIG)
    db_conn.autocommit = True
except psycopg2.Error as e:
    print(f"Failed to connect to the database: {e}")
    exit()

if __name__ == "__main__":

    now = datetime.now(timezone.utc)
    create_tables()

    for crypto_id in CRYPTO_LIST:
        print(f"Processing {crypto_id}...")

        missing_hours = get_missing_hours(crypto_id, START_DATE, now)
        print(f"Missing hours for {crypto_id}: {missing_hours}")

        if not missing_hours:
            print(f"All data is up to date for {crypto_id}.")
            continue

        if len(missing_hours) > 1 and all((b - a).seconds <= 3600 for a, b in zip(missing_hours, missing_hours[1:])):
            df = fetch_historical_data(crypto_id, len(missing_hours))
        else:
            df = fetch_specific_historical_hours(crypto_id, missing_hours)

        #print(df)

        if not df.empty:
            load_to_db_historical(df, crypto_id, db_conn)
        else:
            print(f"No data fetched for {crypto_id}.")

        is_consistent = check_consistency(crypto_id, START_DATE)
        print(f"Data consistency for {crypto_id}: {'consistent' if is_consistent else 'inconsistent'}")


