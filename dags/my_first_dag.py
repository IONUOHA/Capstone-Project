from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'test_project',
    default_args = default_args,
    description = 'an example DAG',
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
   ##def task 1
   start_task = DummyOperator(
      task_id = 'Pipeline_start'
   )

   # def task 2
   wait_task = BashOperator(
      task_id = 'wait',
      bash_command = 'sleep 10'
   ) 

   #def task 3
   end_task = DummyOperator(
      task_id = 'End_pipeline',
   )

   ## set dependencies
   start_task >> wait_task >> end_task  
