from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.batch_etl import run_etl
from scripts.load_to_db import load_processed_data

default_args = {
    'owner': 'pedal_pulse',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'ingest_bike_data',
    default_args=default_args,
    description='Ingest and process bike sharing data',
    schedule_interval='0 2 * * *',  
    catchup=False,
    tags=['bike-sharing', 'etl', 'data-ingestion'],
)

# Task 1: Run batch ETL
run_batch_etl = PythonOperator(
    task_id='run_batch_etl',
    python_callable=run_etl,
    dag=dag,
)

# Task 2: load data to database
load_to_database = PythonOperator(
    task_id='load_to_database',
    python_callable=load_processed_data,
    dag=dag,
)

# Task 3: Validate data quality
validate_data = PostgresOperator(
    task_id='validate_data',
    postgres_conn_id='pedal_pulse_db',
    sql="""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(DISTINCT start_station_id) as unique_start_stations,
        COUNT(DISTINCT end_station_id) as unique_end_stations,
        AVG(trip_duration_minutes) as avg_duration
    FROM bike_trips 
    WHERE DATE(created_at) = CURRENT_DATE
    """,
    dag=dag,
)

# Task 4: send notification on completion
notify_completion = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Data ingestion completed successfully"',
    dag=dag,
)

# define task dependencies
run_batch_etl >> load_to_database >> validate_data >> notify_completion

# DAG for model predictions
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bike_sharing_predictions',
    default_args=default_args,
    description='Generate predictions using trained ML model',
    schedule_interval='@hourly',
    catchup=False
)

# generate predictions
generate_predictions = SparkSubmitOperator(
    task_id='generate_predictions',
    application='pipeline/predict.py',  
    conn_id='spark_default',
    conf={
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g'
    },
    dag=dag
)
# Define task dependencies
generate_predictions 