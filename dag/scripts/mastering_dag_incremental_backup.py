import logging
from scripts.db_functions import (
# from db_functions import (
    get_pg_connection,
    get_last_run_date,
    fetch_new_data,
    update_last_run_date,
    insert_last_run_date,
)
from datetime import datetime, timezone
from scripts.s3_functions import upload_data_to_s3
# from s3_functions import upload_data_to_s3
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRESQL_CONFIG = {
    "host": "ep-damp-dream-a4l97lho.us-east-1.aws.neon.tech",
    "user": "airflow_dag_owner",
    "password": "XwHpzC7YyQ2u",
    "database": "airflow_dag",
    "port": 5432,
}

def incremental_etl(table_name: str, destination_path: str, conn_params: dict):
    conn = None
    try:
        conn = get_pg_connection(conn_params)

        # Step 1: Fetch last run date
        last_run_date = get_last_run_date(conn, table_name)
        if not last_run_date:
            logger.warning(
                f"No last run date found for {table_name}. Defaulting to epoch."
            )

            last_run_date = datetime(1970, 1, 1) #, tzinfo=timezone.utc)
            run_date_insert_flag = True
        else:
            run_date_insert_flag = False

        # Step 2: Fetch new data
        new_data = fetch_new_data(conn, table_name, last_run_date)
        current_date = datetime.now()
        if new_data.empty:
            if run_date_insert_flag:
                insert_last_run_date(conn, current_date, table_name)
            else:
                # Update last run date
                update_last_run_date(conn, current_date, table_name)
            logger.info(f"No new data found for {table_name} since {last_run_date}.")
            return

        upload_data_to_s3(destination_path, table_name, new_data, current_date)

        if run_date_insert_flag:
            insert_last_run_date(conn, current_date, table_name)
        else:
            # Update last run date
            update_last_run_date(conn, current_date, table_name)

    except Exception as e:
        logger.error(f"ETL process failed: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    incremental_etl("customer_information", 'airflow-destination-data', POSTGRESQL_CONFIG)