from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
from airflow.models import Variable
from scripts.incremental_backup import incremental_etl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# PostgreSQL connection parameters
POSTGRESQL_CONFIG = {
    "host": Variable.get("HOST"),
    "user": Variable.get("USER"),
    "password": Variable.get("PASSWORD"),
    "database": Variable.get("DB_NAME"),
    "port": 5432,
}

DESTINATION_BUCKET = "airflow-destination-data"

default_args = {"owner": "airflow", "start_date": datetime(2024, 8, 15), "retries": 5}

with DAG(
    dag_id="incremental_loading_dag",
    schedule_interval="45 15 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    table_names = [
        "billing",
        "customer_information",
        "device_information",
        "plans",
        "customer_rating",
    ]

    # # Define dependencies dynamically
    # dependencies = {
    #     "customer_information": ["billing", "device_information", "customer_rating"]
    # }

    python_operators_dag = {}
    for table_name in table_names:
        python_operators_dag[table_name] = PythonOperator(
            task_id=f"process_{table_name}",
            python_callable=incremental_etl,
            op_args=[table_name, DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

    # Set dependencies - 1 way
    # python_operators_dag["customer_information"] >> [
    #     python_operators_dag["billing"],
    #     python_operators_dag["device_information"],
    #     python_operators_dag["customer_rating"],
    # ]
    # python_operators_dag["plans"]

    # Set dependencies - 2 way
    python_operators_dag["customer_information"] >> python_operators_dag["billing"]
    python_operators_dag["customer_information"] >> python_operators_dag["device_information"]
    python_operators_dag["customer_information"] >> python_operators_dag["customer_rating"]
    python_operators_dag["plans"]
