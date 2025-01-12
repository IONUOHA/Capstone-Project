from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from capstone_project.etl import extract, transform, load  # Import your ETL functions
from datetime import datetime, timedelta


## setting default arguments
default_args ={
    'owner': 'Ike_Onuoha',
    'start_date': datetime(year=2025, month=1, day=12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,  
}


## istantiate the DAG
with DAG(
    'capstone_project_etl',
    default_args = default_args,
    description = 'Capstone project',
    schedule_interval = '0 0 * * *',
    catchup = False
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