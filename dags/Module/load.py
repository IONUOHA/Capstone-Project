## set up the the DB client Library
%pip install psycopg2
! pip install sqlalchemy
%pip install python-dotenv
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
# loading my credentials
load_dotenv()
# credentials
host = os.getenv('db_host')
username = os.getenv ('db_user')
password = os.getenv ('db_password')
port = os.getenv ('db_port')
db_name = os.getenv ('db_name')
# Creating a SQL alchemy engine
database_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}"
engine = create_engine(database_url)
#loading Dataframe to PostgresQL table
table_name = "employee"
# write the Dataframe to postgres
df_emp.to_sql(table_name, engine, if_exists='replace', index=False)
table_name = "title"
# write the Dataframe to postgres
df_title.to_sql(table_name, engine, if_exists='replace', index=False)
table_name = "agency"
# write the Dataframe to postgres
df_agency.to_sql(table_name, engine, if_exists='replace', index=False)
table_name = "nyc_payroll_combined"
# write the Dataframe to postgres
nyc_payroll_combined.to_sql(table_name, engine, if_exists='replace', index=False)
