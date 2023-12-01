from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.python_operator import PythonOperator

from query_functions import save_track_ids, get_audio_features


def query_data():
    save_track_ids()
    print("now entering get_audio_features")
    get_audio_features()


with DAG(
        dag_id='spotify_data_query',
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2020, 11, 1),
        },
        catchup=False,
) as dag:
    query_data_task = PythonOperator(
        task_id="query_data",
        python_callable=query_data
    )

    # Task to create the target HDFS directory
    create_hdfs_dir = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_track_data_dir',
        directory='/user/hadoop/spotify/track_data/raw/{{ ds_nodash }}',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    # Task to upload the local audio_features.json to HDFS
    upload_to_hdfs = HdfsPutFileOperator(
        task_id='upload_audio_features_to_hdfs',
        local_file='/home/airflow/airflow/dags/data/audio_features.json',
        remote_file='/user/hadoop/spotify/track_data/raw/{{ ds_nodash }}/audio_features.json',
        hdfs_conn_id='hdfs',
    )

    # Set the task dependencies
    query_data_task >> create_hdfs_dir
    create_hdfs_dir >> upload_to_hdfs

if __name__ == "__main__":
    dag.cli()
