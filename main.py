# libs
import pandas as pd
import logging
from dotenv import load_dotenv
# modules
from db.db_utils_postgres import prepare_postgres, update_pg_config, postgres_connection, refresh_materialized_view
from db.db_utils_clickhouse import prepare_clickhouse, update_ch_config, clickhouse_connection 
from db.pg_to_ch_pipeline import create_external_pg_table, create_ch_forecast_data_table, insert_from_external
from models.df_and_models_engine import fetch_predict_upload_ts
from config.config_system import PG_DB_CONFIG, CH_DB_CONFIG
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

def prepare_ch_and_pg_containers_users_db_tables():

    #    # guarantee update by .env
    load_dotenv(override=True)

#    # initialize Postgres connection and create tables for fistorical and forecast data
    prepare_postgres()
    postgres_config = update_pg_config(PG_DB_CONFIG)
    postgres_client = postgres_connection(**postgres_config)
    
#    # run Clickhouse container and create user with credentials from config (.env is prioritized)
    prepare_clickhouse()
    clickhouse_config = update_ch_config(CH_DB_CONFIG)
    clickhouse_client = clickhouse_connection(clickhouse_config)
    
    create_external_pg_table(clickhouse_client, clickhouse_config, postgres_config)
    create_ch_forecast_data_table(clickhouse_client, clickhouse_config)


def execute_data_fetch_and_transfer():

    postgres_config = update_pg_config(PG_DB_CONFIG)
    postgres_client = postgres_connection(**postgres_config)

    clickhouse_config = update_ch_config(CH_DB_CONFIG)
    clickhouse_client = clickhouse_connection(clickhouse_config)

    # get data from API, pass it through models, upload historical and forecast data to Postgres
    fetch_predict_upload_ts(postgres_client) # 
    refresh_materialized_view(postgres_client) # to get actial data before transfer into CH 

    insert_from_external(clickhouse_client, clickhouse_config)
    

#
if __name__ == "__main__":

#    # guarantee update by .env
    load_dotenv(override=True)

    prepare_ch_and_pg_containers_users_db_tables()

    execute_data_fetch_and_transfer()



##    # initialize Postgres connection and create tables for fistorical and forecast data
#    prepare_postgres()
#    postgres_config = update_pg_config(PG_DB_CONFIG)
#    postgres_client = postgres_connection(**postgres_config)
#    
##    # run Clickhouse container and create user with credentials from config (.env is prioritized)
#    prepare_clickhouse()
#    clickhouse_config = update_ch_config(CH_DB_CONFIG)
#    clickhouse_client = clickhouse_connection(clickhouse_config)
#    
#    create_external_pg_table(clickhouse_client, clickhouse_config, postgres_config)
#    create_ch_forecast_data_table(clickhouse_client, clickhouse_config)
#
#    # get data from API, pass it through models, upload historical and forecast data to Postgres
#    fetch_predict_upload_ts(postgres_client) # 
#    refresh_materialized_view(postgres_client) # to get actial data before transfer into CH 
#
#    insert_from_external(clickhouse_client, clickhouse_config)



    