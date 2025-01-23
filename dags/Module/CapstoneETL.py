from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

# Load credentials using dotenv
def load_env_variables():
    load_dotenv()
    return {
        "host": os.getenv('host'),
        "username": os.getenv('user'),
        "password": os.getenv('password'),
        "port": os.getenv('port'),
        "db_name": os.getenv('name')
    }

# Define the ETL process
def etl_process():
    # Load environment variables
    creds = load_env_variables()

    # Database connection
    database_url = f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db_name']}"
    engine = create_engine(database_url)

    # Extraction
    nycpayroll_2021_df = pd.read_csv("dags/nycpayroll_2021.csv")
    nycpayroll_2020_df = pd.read_csv("dags/nycpayroll_2020.csv")
    empmaster_df = pd.read_csv("dags/Empmaster.csv")
    titlemaster_df = pd.read_csv("dags/TitleMaster.csv")
    agencymaster_df = pd.read_csv("dags/AgencyMaster.csv")

    # Transformation
    # (Include all transformation steps from earlier code)
    df_2021 = nycpayroll_2021_df.copy()
    df_2020 = nycpayroll_2020_df.copy()
    df_emp = empmaster_df.copy()
    df_title = titlemaster_df.copy()
    df_agency = agencymaster_df.copy()

    # Drop duplicates
    df_2020 = df_2020.drop_duplicates()
    df_2021 = df_2021.drop_duplicates()
    df_emp = df_emp.drop_duplicates()
    df_title = df_title.drop_duplicates()
    df_agency = df_agency.drop_duplicates()

    # Convert columns to appropriate types
    df_2020['AgencyStartDate'] = pd.to_datetime(df_2020['AgencyStartDate'], format='%m/%d/%Y')
    df_2021['AgencyStartDate'] = pd.to_datetime(df_2021['AgencyStartDate'], format='%m/%d/%Y')

    # Clean and rename columns
    df_2021.columns = df_2021.columns.str.lower()
    df_2020.columns = df_2020.columns.str.lower()
    df_agency.columns = df_agency.columns.str.lower()
    df_emp.columns = df_emp.columns.str.lower()
    df_title.columns = df_title.columns.str.lower()

    df_2021.rename(columns={
        'leavestatusasofjune30': 'june30_leavestatus',
        'titledescription': 'title_des',
        'othours': 'ot_hours',
        'totalotpaid': 'total_ot_paid',
        'agencycode': 'agency_id'
    }, inplace=True)

    df_2020.rename(columns={
        'leavestatusasofjune30': 'june30_leavestatus',
        'titledescription': 'title_des',
        'othours': 'ot_hours',
        'totalotpaid': 'total_ot_paid'
    }, inplace=True)

    # Handle missing values
    df_title.dropna(inplace=True)

    # Add full name column
    df_2021['full_name'] = df_2021['lastname'] + ' ' + df_2021['firstname']
    df_2020['full_name'] = df_2020['lastname'] + ' ' + df_2020['firstname']
    df_emp['full_name'] = df_emp['lastname'] + ' ' + df_emp['firstname']

    # Drop unnecessary columns
    df_2021.drop(columns=['firstname', 'lastname'], inplace=True)
    df_2020.drop(columns=['firstname', 'lastname'], inplace=True)
    df_emp.drop(columns=['firstname', 'lastname'], inplace=True)

    # Rename and clean final datasets
    df_emp.columns = ['employee_id', 'full_name']
    df_agency.columns = ['agency_id', 'agency_name']
    df_title.columns = ['title_code', 'job_title']

    # Combine datasets and filter out unwanted fiscal years
    nyc_payroll_combined = pd.concat([df_2020, df_2021], axis=0)
    nyc_payroll_combined.reset_index(drop=True, inplace=True)

    nyc_payroll_combined = nyc_payroll_combined[
        ~nyc_payroll_combined['fiscalyear'].isin([1998, 1999])
    ]

    # Rename columns for final consistency
    nyc_payroll_combined.rename(columns={
        'fiscalyear': 'fiscal_year',
        'payrollnumber': 'payroll_number',
        'agencyname': 'agency_name',
        'agencyid': 'agency_id',
        'total_ot_paid': 'total_ot',
        'employeeid': 'employee_id',
        'basesalary': 'base_salary'
    }, inplace=True)

    # Load to PostgreSQL
    df_emp.to_sql('employee', engine, if_exists='replace', index=False)
    df_title.to_sql('title', engine, if_exists='replace', index=False)
    df_agency.to_sql('agency', engine, if_exists='replace', index=False)
    nyc_payroll_combined.to_sql('nyc_payroll_combined', engine, if_exists='replace', index=False)

## setting default arguments
default_args ={
    'owner': 'Ike_Onuoha',
    'start_date': datetime(year=2025, month=1, day=23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,  
}


## istantiate the DAG
with DAG(
    'Capstone_project',
    default_args = default_args,
    description = 'Capstone DAG',
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
# ETL task
    etl_task = PythonOperator(
        task_id='etl_process',
        python_callable=etl_process
    )

# Set dependencies (if any)
etl_task


