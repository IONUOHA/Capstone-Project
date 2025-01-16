from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

def extract(**kwargs):
    print("Extracting data...")
    # Your extraction logic here
nycpayroll_2021_df = pd.read_csv("nycpayroll_2021.csv")
nycpayroll_2020_df = pd.read_csv("nycpayroll_2020.csv")
empmaster_df = pd.read_csv("Empmaster.csv")
TitleMaster_df = pd.read_csv("TitleMaster.csv")
Agencymaster_df = pd.read_csv("AgencyMaster.csv")
def transform(**kwargs):
    print("Transforming data...")
    # Your transformation logic here

def load(**kwargs):
    print("Loading data...")
    # Your loading logic here


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
      task_id = 'Extract'
      python_callable=extract
   )

   # def task 2
   Transform_task = PythonOperator(
      task_id = 'Transform',
      python_callable=transform
      bash_command = 'sleep 10'
   ) 

   #def task 3
   load_task = pythonOperator(
      task_id = 'load',
      python_callable=load
   )

   ## set dependencies
   extract_task >> transform_task >> load_task  