from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from Capstone_project import extract, transform, load  # Import your ETL functions

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 12),  # Start date for the DAG
    'retries': 1,  # Number of retries for failed tasks
}

# Define the DAG
with DAG(
    'capstone_etl_dag',  # Unique name for your DAG
    default_args=default_args,
    description='ETL Pipeline for Capstone Project',
    schedule_interval='@daily',  # Run the DAG daily
    catchup=False,  # Skip missed runs
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,  # Calls the `extract()` function
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,  # Calls the `transform()` function
    )

    # Task 3: Load data
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,  # Calls the `load()` function
    )

    # Define task dependencies: Extract → Transform → Load
    extract_task >> transform_task >> load_task