# libs
import pandas as pd
import importlib
import logging
from datetime import datetime, timezone, timedelta
# modules
from db_utils_postgres import init_postgres_and_create_tables
from db_utils_clickhouse import init_clickhouse_with_user
from df_and_models_engine import fetch_predict_upload_ts
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("forecrypt.log")
    ]
)
logger = logging.getLogger("forecrypt")
# ---------------------------------------------------------
if __name__ == "__main__":

    # run Clickhouse container and create user with credentials from config (.env is prioritized)
    clickhouse_client = init_clickhouse_with_user()
    postgres_client = init_postgres_and_create_tables()
    
    # get data from API, pass it through models, upload historical and forecast data to Postgres
    fetch_predict_upload_ts(postgres_client)