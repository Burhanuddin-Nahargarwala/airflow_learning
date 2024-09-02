from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


default_args = {"start_date": datetime(2023, 1, 1), "schedule_interval": "@daily"}

# DAG 1
with DAG(
    "multiple_dag_1", default_args=default_args, catchup=False, tags=["multiple-dags"]
) as dag1:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end

# DAG 2
with DAG(
    "multiple_dag_2",
    default_args=default_args,
    catchup=False,
) as dag2:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
