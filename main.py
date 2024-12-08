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

# create database connection
try:
    db_conn = psycopg2.connect(**DB_CONFIG)
    db_conn.autocommit = True
except psycopg2.Error as e:
    print(f"Failed to connect to the database: {e}")
    exit()

if __name__ == "__main__":
    # Step 0: create tables
    create_tables()
    print("tables created or already exist.")

    now = datetime.now(timezone.utc)  # current time as a timestamp

    for crypto_id in CRYPTO_LIST:
        print(f"Processing {crypto_id}...")

        # Step 1: get missing hours
        
        missing_hours = get_missing_hours(crypto_id, START_DATE, now)
        print(f"Missing hours for {crypto_id}: {', '.join([hour.strftime('%Y-%m-%d %H:%M:%S') for hour in missing_hours])}")

        if not missing_hours:
            print(f"All data is up to date for {crypto_id}.")
            continue

        # Step 2: check for gaps in the missing hours
        prev_hour = None
        has_gaps = False

        for hour in missing_hours:
            if prev_hour and (hour - prev_hour).total_seconds() // 3600 >= 2:
                has_gaps = True
                break
            prev_hour = hour

        # Step 3: Fetch data and load into DB
        if not has_gaps:
            # If no gaps, fetch data for the continuous range
            print(f"Fetching historical data for continuous range for {crypto_id}...")
            df = fetch_historical_data(crypto_id, len(missing_hours))  # Save the result to df
        else:
            # If gaps exist, fetch specific hours
            print(f"Fetching specific hours for {crypto_id}...")
            df = fetch_specific_historical_hours(crypto_id, missing_hours)  # Save the result to df

        # Step 4: Load data into the database
        print(df)
        print(df.dtypes)  # убедись, что типы колонок совпадают с типами столбцов в базе
        print(df.head())  # убедись, что данные загружаются правильно
        #print(df.isna().sum())
        load_to_db_historical(df, crypto_id, db_conn)

        # Step 5: Check data consistency
        is_consistent = check_consistency(crypto_id, START_DATE)
        print(f"Data consistency for {crypto_id}: {'consistent' if is_consistent else 'inconsistent'}")
