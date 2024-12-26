from datetime import datetime, timezone
import psycopg2
from db_utils import (
    create_tables,
    get_missing_hours,
    load_to_db_historical,
    check_consistency
)
from get_data import fetch_specific_historical_hours, fetch_historical_data
from config import CRYPTO_LIST, START_DATE, DB_CONFIG
from models import arima_model, ets_model, theta_model

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


