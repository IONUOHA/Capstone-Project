from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from etl_pipeline.extract import extract_data
from etl_pipeline.transform import transform_data
from etl_pipeline.load import load_data
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
    'Capstone_projectETL',
    default_args = default_args,
    description = 'An ETL project DAG',
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
   ##def task 1
   Extract_task = PythonOperator(
      task_id = 'Extract_data'
      python_callable=extract_data
   )

   # def task 2
   Transform_task = PythonOperator(
      task_id = 'transform_data',
      python_callable=transform_data
      bash_command = 'sleep 10'
   ) 

   #def task 3
   load_task = pythonOperator(
      task_id = 'load_data',
      python_callable=load
   )

   ## set dependencies
   extract_task >> transform_task >> load_task  