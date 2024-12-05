import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from dotenv import load_dotenv


class WeeklyExchangeRateChecker:

    def __init__(self):
        # Load environment variables from .env
        load_dotenv()

        self.db_server = os.getenv("SERVER")
        self.db_database = os.getenv("DATABASE")
        self.db_port = os.getenv("PORT")
        self.db_driver = os.getenv("DRIVER")
        self.db_username = os.getenv("USERNAME")
        self.db_password = os.getenv("PASSWORD")

        # SQL Server connection string
        self.connection_url = (
            f"mssql+pyodbc://{self.db_username}:{self.db_password}@{self.db_server}:{self.db_port}/"
            f"{self.db_database}?driver={self.db_driver.replace(' ', '+')}"
        )

    def calculate_weekly_averages(self):
        """Fetches and calculates weekly exchange rate averages from the database."""
        # Connect to the database
        engine = create_engine(self.connection_url)
        query = """
                  WITH WeeklyData AS (
                    SELECT 
                        DATEPART(WEEK, [date]) AS WeekNumber,
                        YEAR([date]) AS YearNumber,
                        AVG([AUD TO LKR]) AS Avg_AUD_to_LKR,
                        AVG([AUD TO USD]) AS Avg_AUD_to_USD
                    FROM dbo.EXCHANGE_RATE
                    GROUP BY YEAR([date]), DATEPART(WEEK, [date])),
                WeeklyComparison AS (
                    SELECT 
                        cur.YearNumber,
                        cur.WeekNumber,
                        cur.Avg_AUD_to_LKR AS Current_AUD_to_LKR,
                        prev.Avg_AUD_to_LKR AS Previous_AUD_to_LKR,
                        cur.Avg_AUD_to_USD AS Current_AUD_to_USD,
                        prev.Avg_AUD_to_USD AS Previous_AUD_to_USD
                    FROM WeeklyData cur
                    LEFT JOIN WeeklyData prev
                    ON cur.YearNumber = prev.YearNumber AND cur.WeekNumber = prev.WeekNumber + 1
                )
                SELECT 
                *
                    
                    FROM WeeklyComparison
                WHERE 
                (ABS(Current_AUD_to_LKR - Previous_AUD_to_LKR) > 0.06 OR 
                ABS(Current_AUD_to_USD - Previous_AUD_to_USD) > 0.06)
                AND WeekNumber = DATEPART(WEEK, GETDATE()) 
                AND YearNumber = YEAR(GETDATE());
        """
        data = pd.read_sql_query(query, engine)
       
        engine.dispose()
        return data
    
    def send_email_with_airflow_operator(self, changes, dag):
    
        subject = "Weekly Exchange Rate Alert"
         # Convert the DataFrame to an HTML table
        html_table = changes.to_html(index=False, border=0, classes='dataframe')

        # Include the table in the email body
        body = f"""
                <html>
                <body>
                <h2>Significant changes detected in weekly exchange rates:</h2>
                     {html_table}
                </body>
                </html>
                    """
        try:

              # Create the email task using Airflow's EmailOperator
            email_task = EmailOperator(
            task_id="send_email_alert",
            to="nimeshaamarasingha@gmail.com",  # Replace with the recipient's email
            subject=subject,
            html_content=body,
            dag=dag,  # Ensure the correct DAG object is passed
            )
            email_task.execute(context={})  # Provide an empty context or fill it as needed

        except AirflowException as e:
            # Handle specific Airflow exceptions
            raise ValueError(f"Failed to send email due to AirflowException: {str(e)}")
        
        except Exception as e:
        # Handle general exceptions
            raise ValueError(f"An error occurred while sending the email: {str(e)}")


    def check_and_alert(self):
        """Check for significant exchange rate changes and send email alert."""
        changes = self.calculate_weekly_averages()
        print(changes)
        if not changes.empty:
            self.send_email_with_airflow_operator(changes,dag)


# Define the default_args dictionary outside the class (global scope)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG in the global scope
dag = DAG(
    'WEEKLY_EXCHANGE_RATE_MONITORING',
    default_args=default_args,
    description='Monitor weekly exchange rates and send alerts',
    schedule_interval='@weekly',
    start_date=datetime(2024, 12, 5),
    catchup=False,
)

# Define the PythonOperator that runs the check_and_alert method
def alert_exchange_rate_changes(**kwargs):
    exchange_checker = WeeklyExchangeRateChecker()
    exchange_checker.check_and_alert()


# Define the task that runs the alert_exchange_rate_changes function
with dag:
    check_and_alert_task = PythonOperator(
        task_id='email_alerts_for_exchange_rate',
        python_callable=alert_exchange_rate_changes,
        provide_context=True,
    )
    
    # Define the task dependencies
    check_and_alert_task
