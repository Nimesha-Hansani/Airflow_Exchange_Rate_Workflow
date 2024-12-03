
import pandas as pd
import requests
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

db_server = os.getenv("SERVER")
db_database = os.getenv("DATABASE")
db_port = os.getenv("PORT")
db_driver = os.getenv("DRIVER")
db_username = os.getenv("USERNAME")
db_password = os.getenv("PASSWORD")

connection_string = (
    f"DRIVER={{{db_driver}}};SERVER={db_server},{db_port};DATABASE={db_database};UID={db_username};PWD={db_password}"
)

def connect_to_sql():
    try:
        # Attempt to establish a connection
        conn = pyodbc.connect(connection_string)
        print("SQL Server connection successful.")
        conn.close()
    except Exception as e:
        print(f"SQL Server connection failed: {e}")
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'check_sql_connection',
    default_args=default_args,
    description='A simple DAG to check SQL Server connection',
    schedule_interval='0 9 * * *',
    start_date=datetime(2024, 12, 3),
    catchup=False,
)

# Define the PythonOperator to check SQL connection
check_sql_task = PythonOperator(
    task_id='check_sql_connection',
    python_callable=connect_to_sql,
    dag=dag,
)





       