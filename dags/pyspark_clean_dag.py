from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Define the Airflow DAG
with DAG(
    dag_id='pyspark_clean',
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2020, 11, 1),
    },
    catchup=False,
) as dag:
    # PySpark data cleaning task using SparkSubmitOperator
    pyspark_clean = SparkSubmitOperator(
        task_id="pyspark_clean_script.py",
        conn_id='spark',
        application="/home/airflow/airflow/python/pyspark_clean_script.py",
        name='pyspark_clean',
        verbose=True,
    )

# Set task dependencies
pyspark_clean