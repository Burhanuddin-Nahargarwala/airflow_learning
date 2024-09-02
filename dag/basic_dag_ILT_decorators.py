from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Define the DAG with the @dag decorator
@dag(
    dag_id="example_dag_decorator",
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
    def process_data(data):
        print(f"{data} processed")
        return f"{data} processed"
    
    @task
    def load_data(processed_data):
        print(f"{processed_data} loaded")
    
    # Set task dependencies
    data = extract_data()
    processed_data = process_data(data)
    load_data(processed_data)


# Instantiate the DAG
dag_instance = my_dag()
