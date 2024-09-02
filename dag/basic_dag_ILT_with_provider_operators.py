## There is no direct operator to fetch transfer data from RDS to S3


# from airflow import DAG
# from airflow.providers.amazon.aws.transfers.postgres_to_s3 import PostgresToS3Operator
# from datetime import datetime
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# DESTINATION_BUCKET = "airflow-destination-data"

# default_args = {"owner": "airflow", "start_date": datetime(2024, 8, 27)}

# dag = DAG(
#     dag_id="basic_dag_ILT_external_operators",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
# )

# # Step 1: Export data from PostgreSQL to S3
# export_customer_information = PostgresToS3Operator(
#     task_id="export_customer_information_to_s3",
#     postgres_conn_id="telecom_airflow",
#     sql="SELECT * FROM customer_information WHERE created_at > '{{ prev_ds }}' OR updated_at > '{{ prev_ds }}'",
#     s3_bucket=DESTINATION_BUCKET,
#     s3_key="customer_information_{{ ds_nodash }}.csv",
#     replace=True,
#     export_format="csv",
#     dag=dag
# )

# logging.info("Customer Information Data Backup is completed!!")

# export_customer_rating = PostgresToS3Operator(
#     task_id="export_customer_rating_to_s3",
#     postgres_conn_id="telecom_airflow",
#     sql="SELECT * FROM customer_rating WHERE created_at > '{{ prev_ds }}' OR updated_at > '{{ prev_ds }}'",
#     s3_bucket=DESTINATION_BUCKET,
#     s3_key="customer_rating_{{ ds_nodash }}.csv",
#     replace=True,
#     export_format="csv",
#     dag=dag
# )

# logging.info("Customer Rating Data Backup is completed!!")

# # Define task dependencies
# export_customer_information >> export_customer_rating