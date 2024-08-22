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
    "host": "telecom.c2b3frbss0vu.ap-south-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "Telecom_Airflow123",
    "database": "wetelco_telecom",
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

    billing = PythonOperator(
        task_id=f"process_billing",
        python_callable=incremental_etl,
        op_args=["billing", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    customer_information = PythonOperator(
        task_id=f"process_customer_information",
        python_callable=incremental_etl,
        op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    device_information = PythonOperator(
        task_id=f"process_device_information",
        python_callable=incremental_etl,
        op_args=["device_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    plans = PythonOperator(
        task_id=f"process_plans",
        python_callable=incremental_etl,
        op_args=["plans", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    customer_rating = PythonOperator(
        task_id=f"process_customer_rating",
        python_callable=incremental_etl,
        op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    # Define task dependencies
    customer_information >> [
        billing, 
        device_information, 
        customer_rating
    ]
    plans
