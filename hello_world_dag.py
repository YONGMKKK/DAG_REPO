# test_dag.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 300,  # 5 minutes
}

# Create a simple function that will be the task
def hello_world():
    print("Hello, world!")

# Instantiate a DAG
dag = DAG(
    'test_hello_world_dag',  # DAG name
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',  # Schedule (daily in this case)
    start_date=datetime(2023, 9, 1),  # Start date (make sure it’s in the past for testing)
    catchup=False,  # Don't run for past dates
)

# Define a task using PythonOperator
hello_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,  # Function to be executed
    dag=dag,  # DAG instance
)
