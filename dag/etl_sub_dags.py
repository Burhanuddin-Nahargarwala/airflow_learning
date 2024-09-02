from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
from scripts.incremental_backup import incremental_etl
from scripts.etl_functions import (
    late_payment_aggregated_view,
    customer_rating_aggregated_view,
    billing_amount_aggregated_view,
)
from airflow.utils.dates import days_ago

# Set up logging
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

# Define the default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "email": ["burhanuddin@mentorskool.com"],
    "email_on_retry": True,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
    "depends_on_past": False,
}

# Function to create SubDAG for Incremental Backup
def incremental_backup_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
        tags=['sub-dags']
    )

    with dag_subdag:
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
    
    return dag_subdag

# Function to create SubDAG for Aggregated View
def aggregated_view_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    )

    with dag_subdag:
        late_payment_analysis_task = PythonOperator(
            task_id="late_payment_analysis",
            python_callable=late_payment_aggregated_view,
            op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
        )

        customer_rating_analysis_task = PythonOperator(
            task_id="customer_rating_analysis",
            python_callable=customer_rating_aggregated_view,
            op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
        )

        billing_amount_analysis_task = PythonOperator(
            task_id="billing_amount_analysis",
            python_callable=billing_amount_aggregated_view,
            op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
        )

        late_payment_analysis_task >> customer_rating_analysis_task >> billing_amount_analysis_task
    
    return dag_subdag

# Main DAG
with DAG(
    dag_id="etl_dag_with_subdags",
    default_args=default_args,
    schedule_interval="30 4 * * 1-5",
    catchup=False,
) as dag:

    # SubDAG for Incremental Backup
    incremental_backup = SubDagOperator(
        task_id="incremental_backup",
        subdag=incremental_backup_subdag("etl_dag_with_subdags", "incremental_backup", default_args),
    )

    # SubDAG for Aggregated View
    aggregated_view = SubDagOperator(
        task_id="aggregated_view",
        subdag=aggregated_view_subdag("etl_dag_with_subdags", "aggregated_view", default_args),
    )

    # Define the dependency between SubDAGs
    incremental_backup >> aggregated_view
