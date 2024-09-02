from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Define the DAG with the @dag decorator
@dag(
    dag_id="dag_decorator_concurrent_tasks",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dag-decorator"]
)
def my_dag():
    
    # Define a task using the @task decorator
    @task
    def extract_data():
        print("data extracted")
        return "data extracted"
    
    @task
    def process_data_1(data):
        print(f"{data} processed by process_data_1")
        return f"{data} processed by process_data_1"
    
    @task
    def process_data_2(data):
        print(f"{data} processed by process_data_2")
        return f"{data} processed by process_data_2"
    
    @task
    def load_data_1(processed_data):
        print(f"{processed_data} loaded by load_data_1")
    
    @task
    def load_data_2(processed_data):
        print(f"{processed_data} loaded by load_data_2")
    
    # Set task dependencies
    data = extract_data()
    
    # Both process_data_1 and process_data_2 execute in parallel
    processed_data_1 = process_data_1(data)
    processed_data_2 = process_data_2(data)
    
    # Both load_data_1 and load_data_2 execute in parallel
    load_data_1(processed_data_1)
    load_data_2(processed_data_2)

# Instantiate the DAG
dag_instance = my_dag()