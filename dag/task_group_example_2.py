from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

with DAG(
    dag_id="dependency_taskgrouptask_and_dagtask",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=['task-groups']
):

    dag_task1 = PythonOperator(
        task_id="python_task_1", python_callable=lambda x: print("Python Task 1!")
    )

    with TaskGroup("group1", default_args={"retries": 3}) as group1:
        task1 = DummyOperator(task_id="task1")
        task2 = BashOperator(
            task_id="task2", bash_command="echo Hello World!", retries=2
        )
        print(task1.retries)  # 3
        print(task2.retries)  # 2

    dag_task1 >> task1
    task1 >> task2
