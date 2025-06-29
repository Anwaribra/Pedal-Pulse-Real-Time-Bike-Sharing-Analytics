from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

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
    'bike_sharing_ml_pipeline',
    default_args=default_args,
    description='Process bike sharing data and train ML models',
    schedule_interval='@daily',
    catchup=False
)

# process data and train model using Spark
process_and_train = SparkSubmitOperator(
    task_id='process_and_train_model',
    application='pipeline/batch_etl.py',
    conn_id='spark_default',
    conf={
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g'
    },
    dag=dag
)

# Define task dependencies
process_and_train 