from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
import boto3
import pandas as pd
import psycopg2
from io import BytesIO

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL configuration
POSTGRESQL_CONFIG = {
    # Your database credentials here
}

DESTINATION_BUCKET = "airflow-destination-data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 15),
}

dag = DAG(
    dag_id="basic_dag_ILT",
    schedule_interval="@daily",
    default_args=default_args,
)

def customer_information():
    try:
        # Establish PostgreSQL connection
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)

        # Step 1: Fetch last run date
        query = "SELECT last_run_date FROM etl_last_run_metadata WHERE table_name = 'customer_information'"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        last_run_date = result[0] if result else datetime(1970, 1, 1)
        run_date_insert_flag = not result

        # Step 2: Fetch new data
        query = """
        SELECT * FROM customer_information 
        WHERE created_at > %s OR updated_at > %s
        """
        df = pd.read_sql(query, conn, params=(last_run_date, last_run_date))
        current_date = datetime.now()

        if df.empty:
            if run_date_insert_flag:
                query = "INSERT INTO etl_last_run_metadata (table_name, last_run_date) VALUES (%s, %s)"
                cursor.execute(query, ('customer_information', current_date))
            else:
                query = "UPDATE etl_last_run_metadata SET last_run_date = %s WHERE table_name = %s"
                cursor.execute(query, (current_date, 'customer_information'))
            conn.commit()
            logger.info(f"No new data found for customer_information since {last_run_date}.")
            return

        # Upload data to S3
        s3 = boto3.client('s3')
        folder_name = 'customer_information'
        response = s3.list_objects_v2(Bucket=DESTINATION_BUCKET, Prefix=f"{folder_name}/")
        if 'Contents' not in response:
            s3.put_object(Bucket=DESTINATION_BUCKET, Key=f"{folder_name}/")
        file_key = f"{folder_name}/customer_information_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=DESTINATION_BUCKET, Key=file_key, Body=csv_buffer.getvalue())
        logger.info(f"Uploaded {file_key} to {DESTINATION_BUCKET}.")

        if run_date_insert_flag:
            query = "INSERT INTO etl_last_run_metadata (table_name, last_run_date) VALUES (%s, %s)"
            cursor.execute(query, ('customer_information', current_date))
        else:
            query = "UPDATE etl_last_run_metadata SET last_run_date = %s WHERE table_name = %s"
            cursor.execute(query, (current_date, 'customer_information'))
        conn.commit()

    except Exception as e:
        logger.error(f"ETL process for customer_information failed: {e}")

    finally:
        if conn:
            conn.close()

def customer_rating():
    try:
        # Establish PostgreSQL connection
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)

        # Step 1: Fetch last run date
        query = "SELECT last_run_date FROM etl_last_run_metadata WHERE table_name = 'customer_rating'"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        last_run_date = result[0] if result else datetime(1970, 1, 1)
        run_date_insert_flag = not result

        # Step 2: Fetch new data
        query = """
        SELECT * FROM customer_rating 
        WHERE created_at > %s OR updated_at > %s
        """
        df = pd.read_sql(query, conn, params=(last_run_date, last_run_date))
        current_date = datetime.now()

        if df.empty:
            if run_date_insert_flag:
                query = "INSERT INTO etl_last_run_metadata (table_name, last_run_date) VALUES (%s, %s)"
                cursor.execute(query, ('customer_rating', current_date))
            else:
                query = "UPDATE etl_last_run_metadata SET last_run_date = %s WHERE table_name = %s"
                cursor.execute(query, (current_date, 'customer_rating'))
            conn.commit()
            logger.info(f"No new data found for customer_rating since {last_run_date}.")
            return

        # Upload data to S3
        s3 = boto3.client('s3')
        folder_name = 'customer_rating'
        response = s3.list_objects_v2(Bucket=DESTINATION_BUCKET, Prefix=f"{folder_name}/")
        if 'Contents' not in response:
            s3.put_object(Bucket=DESTINATION_BUCKET, Key=f"{folder_name}/")
        file_key = f"{folder_name}/customer_rating_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=DESTINATION_BUCKET, Key=file_key, Body=csv_buffer.getvalue())
        logger.info(f"Uploaded {file_key} to {DESTINATION_BUCKET}.")

        if run_date_insert_flag:
            query = "INSERT INTO etl_last_run_metadata (table_name, last_run_date) VALUES (%s, %s)"
            cursor.execute(query, ('customer_rating', current_date))
        else:
            query = "UPDATE etl_last_run_metadata SET last_run_date = %s WHERE table_name = %s"
            cursor.execute(query, (current_date, 'customer_rating'))
        conn.commit()

    except Exception as e:
        logger.error(f"ETL process for customer_rating failed: {e}")

    finally:
        if conn:
            conn.close()

customer_information_task = PythonOperator(
    task_id="process_customer_information",
    python_callable=customer_information,
    dag=dag,
)

customer_rating_task = PythonOperator(
    task_id="process_customer_rating",
    python_callable=customer_rating,
    dag=dag,
)

# Define task dependencies
customer_information_task >> customer_rating_task