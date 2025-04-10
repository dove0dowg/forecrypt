from psycopg2.extras import execute_values
import logging
import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db.db_utils_postgres as db_utils_postgres
from config import PG_DB_CONFIG

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    load_dotenv()
    active_config = db_utils_postgres.update_pg_config(PG_DB_CONFIG)
    conn = db_utils_postgres.postgres_connection(**active_config)
    db_utils_postgres.delete_mv_and_tables(conn)