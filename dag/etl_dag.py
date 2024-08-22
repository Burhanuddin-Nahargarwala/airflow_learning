from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
from airflow.models import Variable
from scripts.incremental_backup import incremental_etl
from scripts.etl_functions import (
    late_payment_analysis,
    customer_rating_analysis,
    billing_amount_analysis,
)


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
    dag_id="etl_dag",
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

    ## Factual Tasks
    late_payment_analysis_task = PythonOperator(
        task_id=f"late_payment_analysis", python_callable=late_payment_analysis
    )

    customer_rating_analysis_task = PythonOperator(
        task_id=f"customer_rating_analysis", python_callable=customer_rating_analysis
    )

    billing_amount_analysis_task = PythonOperator(
        task_id=f"billing_amount_analysis", python_callable=billing_amount_analysis
    )

    # Define task dependencies
    customer_information >> [billing, device_information, customer_rating]
    plans >> billing_amount_analysis
    customer_information >> billing_amount_analysis
    billing >> late_payment_analysis_task
    customer_rating >> customer_rating_analysis_task

