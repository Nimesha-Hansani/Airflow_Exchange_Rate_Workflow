import pandas as pd
import requests
import pyodbc
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.email import send_email
import sqlalchemy
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv('/opt/airflow/.env')


class ExchangeRateDAG:

    def __init__(self):
        
        self.db_server = os.getenv("SERVER")
        self.db_database = os.getenv("DATABASE")
        self.db_port = os.getenv("PORT")
        self.db_driver = os.getenv("DRIVER")
        self.db_username = os.getenv("USERNAME")
        self.db_password = os.getenv("PASSWORD")
        self.API_KEY=os.getenv("API_KEY")
    

        self.connection_string = (
            f"DRIVER={{{self.db_driver}}};SERVER={self.db_server},{self.db_port};DATABASE={self.db_database};UID={self.db_username};PWD={self.db_password}"
        )

        self.connection_url = (
            f"mssql+pyodbc://{self.db_username}:{self.db_password}@{self.db_server}:{self.db_port}/"
            f"{self.db_database}?driver={self.db_driver.replace(' ', '+')}"
        )

        # Default arguments for the DAG
        self.default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email': ['nimeshaamarasingha@gmail.com'],
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        }

         # Define the DAG
        self.dag = DAG(
            'EXCHANGE_RATE_LOAD',
            default_args=self.default_args,
            description='Load data from API to SQL Server',
            schedule_interval='0 9 * * *',
            start_date=datetime(2024, 11, 21),
            catchup=False,
        )

    def send_failure_email(context):

        """Callback function to send failure email."""

        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        error = context.get('exception')
        email_subject = f"Airflow Task Failure: {dag_id}.{task_id}"

        email_body = f"""
                         Task {task_id} in DAG {dag_id} has failed.
                            Error: {error}
                        """
        recipients = ['nimeshaamarasingha@gmail.com']  # Replace with your email(s)
        
        send_email(recipients, email_subject, email_body)

    def get_rates(self,start_date,end_date):
        
        base_url = "https://api.currencylayer.com/timeframe?"
        api_url = f"{base_url}start_date={start_date}&end_date={end_date}"
        params = {"access_key": self.API_KEY, "source": "AUD", "currencies": "LKR,USD"}
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
           response_data = response.json()
        else:
            raise ValueError(f"Error {response.status_code}: {response.text}")
  

        dates = list(response_data['quotes'].keys())
        aud_to_lkr = [response_data['quotes'][date]['AUDLKR'] for date in dates]
        aud_to_usd = [response_data['quotes'][date]['AUDUSD'] for date in dates]


        return pd.DataFrame({
            'Date': dates,
            'AUD TO LKR': aud_to_lkr,
            'AUD TO USD': aud_to_usd,
        })

    def check_table_exists(self, **kwargs):
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            table_name = kwargs['table_name']
            cursor.execute(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
            exists = cursor.fetchone()
            conn.close()
            return "fetch_and_load_initial_data" if not exists else "fetch_and_load_incremental_data"
        except Exception as e:
            raise ValueError(f"Error checking table existence: {e}")
        

    def fetch_and_load_initial_data(self,**kwargs):
        try:
            print(f"Table does not exist or is empty. Loading data...")

            start_date = kwargs['start_date']
            end_date = kwargs['end_date']
            rates_df = self.get_rates(start_date,end_date)
            engine = sqlalchemy.create_engine(self.connection_url)
            table_name = kwargs['table_name']
            rates_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Data loaded successfully into table '{table_name}'.")

        except Exception as e:
            raise ValueError(f"Error on loading table: {e}")
        
    def fetch_and_load_incremental_data(self, **kwargs):
        try:
            # Establish database connection
            engine = sqlalchemy.create_engine(self.connection_url)
            table_name = kwargs['table_name']

             # Query to get the maximum date from the database
            query = f"SELECT MAX([Date]) FROM {table_name}"
            with engine.connect() as connection:
              max_date_result = connection.execute(query).fetchone()
              max_date = max_date_result[0] if max_date_result[0] is not None else None

            # Determine start_date and end_date
            if max_date:
                # Convert max_date (string) to a datetime object
                max_date = datetime.strptime(max_date, '%Y-%m-%d')
                start_date = (max_date + timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                raise ValueError(f"No maximum date found in table {table_name}. Ensure initial data load is completed.")
            
            end_date = datetime.now().strftime('%Y-%m-%d')

            # Fetch incremental data using get_rates
            rates_df = self.get_rates(start_date, end_date)
            if rates_df is None or rates_df.empty:
                print(f"No new data found for the date range {start_date} to {end_date}.")
                return
            # Append new data to the table
            rates_df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Incremental data successfully loaded into table '{table_name}' for the date range {start_date} to {end_date}.")


        except Exception as e:

            print(f"Error in fetch_and_load_incremental_data: {e}")
            raise  # Re-raise the exception for Airflow to handle
        
    def build_dag(self):
        with self.dag:
            check_table = BranchPythonOperator(
                task_id='check_table_exists',
                python_callable=self.check_table_exists,
                op_kwargs={'table_name': 'EXCHANGE_RATE'},
                on_failure_callback=self.send_failure_email

            )

            fetch_initial_data = PythonOperator(
                task_id='fetch_and_load_initial_data',
                python_callable=self.fetch_and_load_initial_data,
                op_kwargs={'table_name': 'EXCHANGE_RATE',
                           'start_date': '2024-01-01',
                           'end_date': '2024-02-01'}
            )

            fetch_incremental_data = PythonOperator(
                task_id='fetch_and_load_incremental_data',
                python_callable=self.fetch_and_load_incremental_data,
                op_kwargs={'table_name': 'EXCHANGE_RATE'},
                on_failure_callback=self.send_failure_email

            )

            check_table >> [fetch_initial_data, fetch_incremental_data]

        return self.dag


# Instantiate and expose the DAG
exchange_rate_dag = ExchangeRateDAG()
dag = exchange_rate_dag.build_dag()
        


    
