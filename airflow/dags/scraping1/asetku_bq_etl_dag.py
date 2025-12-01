from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from services.scraping1.extractor import extract
from operators.scraping1.load_operator import LoadOperator 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 25),
    'retries': 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id='asetku_bq_etl',
    default_args=default_args,
    schedule_interval="0 1 * * *", # daily at 01:00 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["web", "scraping", "beautifulsoup", "bigquery"],
) as dag:

    # Task 1: PythonOperator with function factory (closure-based)
    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=extract,  # injects config via closure
        provide_context=True
    )
    
    load_task = LoadOperator(
        task_id="load_to_bq",
        config_path="config/config_asetku.yaml"
    )

    # DAG dependency
    ingestion_task >> load_task

   
   
   