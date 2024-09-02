from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

DAG_NAME = 'example_subdag_operator'


def subdag(parent_dag_name, child_dag_name, args):
    """
    Generate a DAG to be used as a subdag.

    :param str parent_dag_name: Id of the parent DAG
    :param str child_dag_name: Id of the child DAG
    :param dict args: Default arguments to provide to the subdag
    :return: DAG to use as a subdag
    :rtype: airflow.models.DAG
    """
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval="@daily",
    )

    for i in range(5):
        EmptyOperator(
            task_id=f'{child_dag_name}-task-{i + 1}',
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=days_ago(2),
    schedule_interval="@once",
    tags=['example', 'sub-dags'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', dag.default_args),
    )

    some_other_task = EmptyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', dag.default_args),
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end