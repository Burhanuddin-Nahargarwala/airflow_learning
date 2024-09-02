from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
from scripts.incremental_backup import incremental_etl
from scripts.etl_functions_task_group import customer_rating_analysis, device_analysis

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

DESTINATION_BUCKET = "airflow-destination-data/burhan"
AGGREGATED_VIEW_BUCKET = "airflow-facts-analyses/burhan"

# Default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 8, 28),
    "email": ["burhanuddin@mentorskool.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Instantiate the DAG
with DAG(
    dag_id="etl_nested_tasks_groups",
    default_args=default_args,
    schedule_interval="30 4 * * 1-5",
    catchup=False,
    tags=['sub-dags']
) as dag:

    # Incremental Backup Task Group
    with TaskGroup("incremental_backup") as incremental_backup:
        billing = PythonOperator(
            task_id="process_billing",
            python_callable=incremental_etl,
            op_args=["billing", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        customer_information = PythonOperator(
            task_id="process_customer_information",
            python_callable=incremental_etl,
            op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        device_information = PythonOperator(
            task_id="process_device_information",
            python_callable=incremental_etl,
            op_args=["device_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        plans = PythonOperator(
            task_id="process_plans",
            python_callable=incremental_etl,
            op_args=["plans", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        customer_rating = PythonOperator(
            task_id="process_customer_rating",
            python_callable=incremental_etl,
            op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        customer_information >> [billing, device_information, customer_rating]

    # Aggregated View Task Group
    with TaskGroup("aggregated_view") as aggregated_view:
        # Aggregation Tasks
        with TaskGroup(
            "customer_rating_aggregated_view_group"
        ) as customer_rating_aggregated_view_group:
            customer_rating_analysis_task = PythonOperator(
                task_id="customer_rating_aggregated_view",
                python_callable=customer_rating_analysis,
                op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
            )

        # Aggregation Tasks
        with TaskGroup(
            "device_aggregated_view_task_group"
        ) as device_aggregated_view_task_group:
            device_analysis_task = PythonOperator(
                task_id="device_aggregated_view",
                python_callable=device_analysis,
                op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
            )

    # Set dependencies between Task Groups
    incremental_backup >> [
        customer_rating_aggregated_view_group,
        device_aggregated_view_task_group,
    ]
