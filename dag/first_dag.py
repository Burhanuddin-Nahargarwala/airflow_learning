from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Task 1: Create Customer Data
def create_customer_data():
    customer_data = {
        'customer_id': range(1, 11),
        'customer_name': ['John Doe', 'Jane Smith', 'Emily Davis', 'Michael Johnson', 'Chris Lee', 
                          'Jessica Brown', 'David Wilson', 'Sarah Taylor', 'Daniel Anderson', 'Laura Martin'],
        'email': ['john@example.com', 'jane@example.com', 'emily@example.com', 'michael@example.com', 
                  'chris@example.com', 'jessica@example.com', 'david@example.com', 'sarah@example.com', 
                  'daniel@example.com', 'laura@example.com']
    }
    df_customers = pd.DataFrame(customer_data)
    df_customers.to_csv('/tmp/customers.csv', index=False)  # Saving to a temporary file
    logger.info("Customer data created and saved to /tmp/customers.csv")

# Task 2: Create Order Data
def create_order_data():
    order_data = {
        'order_id': range(101, 151),
        'customer_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'product': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch', 
                    'Camera', 'Printer', 'Monitor', 'Keyboard', 'Mouse',
                    'Speaker', 'External Hard Drive', 'Router', 'TV', 'Smartphone',
                    'Laptop', 'Smartwatch', 'Headphones', 'Tablet', 'Printer',
                    'Camera', 'Monitor', 'Keyboard', 'Mouse', 'TV',
                    'Speaker', 'External Hard Drive', 'Router', 'TV', 'Smartwatch',
                    'Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch',
                    'Camera', 'Printer', 'Monitor', 'Keyboard', 'Mouse',
                    'Speaker', 'External Hard Drive', 'Router', 'TV', 'Smartphone',
                    'Laptop', 'Smartwatch', 'Headphones', 'Tablet', 'Printer'],
        'amount': [1000, 500, 300, 100, 200, 
                   800, 150, 200, 50, 25,
                   75, 100, 125, 700, 600,
                   1100, 210, 150, 330, 190,
                   850, 220, 60, 35, 750,
                   90, 140, 130, 710, 240,
                   1150, 520, 320, 130, 210,
                   820, 250, 280, 55, 29,
                   78, 105, 145, 720, 510,
                   1120, 230, 180, 360, 220]
    }
    df_orders = pd.DataFrame(order_data)
    df_orders.to_csv('/tmp/orders.csv', index=False)  # Saving to a temporary file
    logger.info("Order data created and saved to /tmp/orders.csv")

# Task 3: Perform Analysis on Customer and Order Data
def perform_analysis():
    df_customers = pd.read_csv('/tmp/customers.csv')
    df_orders = pd.read_csv('/tmp/orders.csv')
    
    # Merging customer and order data
    merged_data = pd.merge(df_customers, df_orders, on='customer_id')
    
    # Simple analysis: Total amount spent by each customer
    analysis = merged_data.groupby('customer_name')['amount'].sum().reset_index()
    
    logger.info("Analysis result:")
    logger.info(analysis)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 25),
    'retries': 1,
}

with DAG(dag_id='customer_order_analysis_first_dag',
         default_args=default_args,
         schedule_interval='30 9,21 * * *',
         catchup=False) as dag:

    create_customer_data_task = PythonOperator(
        task_id='create_customer_data',
        python_callable=create_customer_data,
    )

    create_order_data_task = PythonOperator(
        task_id='create_order_data',
        python_callable=create_order_data,
    )

    perform_analysis_task = PythonOperator(
        task_id='perform_analysis',
        python_callable=perform_analysis,
    )

    # Setting up dependencies
    [create_customer_data_task, create_order_data_task] >> perform_analysis_task
