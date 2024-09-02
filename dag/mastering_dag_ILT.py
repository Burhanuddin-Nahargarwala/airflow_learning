from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
from scripts.mastering_dag_incremental_backup import incremental_etl
from scripts.mastering_dag_fact_analyses import customer_rating_aggregated_view
import pendulum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# PostgreSQL connection parameters
POSTGRESQL_CONFIG = {
    "host": "ep-damp-dream-a4l97lho.us-east-1.aws.neon.tech",
    "user": "airflow_dag_owner",
    "password": "XwHpzC7YyQ2u",
    "database": "airflow_dag",
    "port": 5432,
}

DESTINATION_BUCKET = "airflow-destination-data/burhan"

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(
        2024, 8, 26, tz="Asia/Kolkata"
    ),
    "schedule_interval": "@daily",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email": ["burhanuddin@mentorskool.com", "amit@mentorskool.com"],
    "email_on_retry": True,
    "email_on_failure": True,
}

with DAG(
    dag_id="mastering_dag_ILT",
    schedule_interval="45 15 * * *",
    default_args=default_args,
    catchup=True,
    tags=["mastering_dag", "analyses_dag", "incremental_backup_dag"]
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
    AGGREGATED_VIEW_BUCKET = "airflow-facts-analyses/burhan"
    customer_rating_aggregated_view_task = PythonOperator(
        task_id=f"customer_rating_aggregated_view",
        python_callable=customer_rating_aggregated_view,
        op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
    )

    # Define task dependencies
    customer_information >> [billing, device_information, customer_rating]
    customer_rating >> customer_rating_aggregated_view_task
