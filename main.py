#import pandas as pd
import psycopg2
from datetime import datetime, timezone
from db_utils import (
    create_tables,
    get_missing_hours,
    load_to_db_historical,
    check_consistency
)
from data.get_data import fetch_specific_hours, fetch_historical_data
from config import CRYPTO_LIST, START_DATE, DB_CONFIG

# create database connection
try:
    db_conn = psycopg2.connect(**DB_CONFIG)
    db_conn.autocommit = True
except psycopg2.Error as e:
    print(f"Failed to connect to the database: {e}")
    exit()

if __name__ == "__main__":
    # step 0: create tables
    create_tables()
    print("tables created or already exist.")

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    for crypto_id in CRYPTO_LIST:
        print(f"Processing {crypto_id}...")

        # step 1: check for missing hours
        missing_hours = get_missing_hours(crypto_id, START_DATE, now)
        print(f"missing hours for {crypto_id}: {missing_hours}")

        # step 2: check data consistency
        is_consistent = check_consistency(crypto_id, START_DATE)
        print(f"data consistency for {crypto_id}: {'consistent' if is_consistent else 'inconsistent'}")

        # step 3: fetch and load data
        if is_consistent:
            hours = (datetime.strptime(now, "%Y-%m-%d %H:%M:%S") - datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S")).total_seconds() // 3600
            historical_df = fetch_historical_data(crypto_id, int(hours), api_key="YOUR_API_KEY")
        else:
            if missing_hours:
                historical_df = fetch_specific_hours(crypto_id, missing_hours, api_key="YOUR_API_KEY")
            else:
                print(f"No missing data for {crypto_id}. Skipping.")
                continue

        # load data to the database
        if not historical_df.empty:
            load_to_db_historical(historical_df, crypto_id, db_conn)
            print(f"{crypto_id}: data loaded into the database.")
        else:
            print(f"{crypto_id}: no data to load.")