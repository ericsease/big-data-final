import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

# Define the Airflow DAG
with DAG(
        dag_id='html_frontend',
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
            "start_date": datetime(2023, 1, 1),
        },
        catchup=False,
) as dag:
    # Reference the previous DAG's Hive table in the new DAG
    hive_check_music_and_genres = HiveOperator(
        task_id='hive_check_music_and_genres',
        hql='SELECT * FROM music_and_genres LIMIT 5',
        hive_cli_conn_id='beeline',
        dag=dag,
    )


    def run_flask_app():
        subprocess.run(['python3', '/home/airflow/airflow/dags/app/run_flask_app.py'])


    run_flask_app_task = PythonOperator(
        task_id='run_flask_app',
        python_callable=run_flask_app,
        dag=dag,
    )

    hive_check_music_and_genres >> run_flask_app_task
