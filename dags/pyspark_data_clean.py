import json
from datetime import datetime, timedelta

import findspark
import pandas as pd
from airflow import DAG
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession


# Function to combine the titles and audio features and drop unnecessary columns
def clean_data(hdfs_directory):
    findspark.init()
    findspark.find()
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

    # Construct paths for track_ids.json and audio_features.json
    track_ids_path = '/user/hadoop/spotify/track_data/raw/track_ids/track_ids.json'
    audio_features_path = f'{hdfs_directory}audio_features/audio_features.json'
    print(f'TRACK IDS: {track_ids_path}')
    print(f'AUDIO FEATURES: {audio_features_path}')
    try:
        # Read the JSON file into a PySpark DataFrame using the json function
        track_ids_df = spark.read.json(track_ids_path)

        # Show the first five rows of the DataFrame
        track_ids_df.show(5)

        # Read the JSON file into a PySpark DataFrame using the json function
        audio_features_df = spark.read.json(audio_features_path)

        # Drop unnecessary columns
        columns_to_drop = ['type', 'uri', 'track_href', 'analysis_url']
        audio_features_df = audio_features_df.drop(*columns_to_drop)

        # Show the first five rows of the DataFrame
        audio_features_df.show(5)

        # Merge the DataFrames on the 'id' column
        merged_df = track_ids_df.join(audio_features_df, on='id', how='inner')

        # Show the merged DataFrame
        merged_df.show(5)

        # Save DataFrame to JSON
        outfile = "/home/airflow/airflow/dags/data/cleaned_combined_data.json"
        outfile_hdfs = 'hdfs:///user/hadoop/spotify/track_data/final/model_data/cleaned_combined_data.json'

        local_data = merged_df.collect()
        pd_df = pd.DataFrame(local_data)
        pd_df.to_json(outfile, orient='records', lines=True)

        # Save the merged DataFrame as JSON to the HDFS location
        merged_df.write.json(outfile_hdfs, mode='overwrite')

    except Exception as e:
        print(f"Error reading JSON files: {e}")

    # Stop the Spark session
    spark.stop()


def preprocess_track_ids(input_file, output_file):
    with open(input_file, 'r') as infile:
        data = json.load(infile)

    # Convert the dictionary to a list of dictionaries
    records = [{"id": key, "title": value} for key, value in data.items()]

    with open(output_file, 'w') as outfile:
        json.dump(records, outfile)


def convert_json(input_json):
    output_list = []

    for track_id, track_data in input_json.items():
        if track_data is not None:
            track_info = {"id": track_id}
            track_info.update(track_data)
            output_list.append(track_info)

    return output_list


def preprocess_audio_features(input_file, output_file):
    print(input_file)
    with open(input_file, 'r') as infile:
        input_json = json.load(infile)

    if not isinstance(input_json, dict):
        raise ValueError("Input JSON is not a dictionary")
    elif input_json is None:
        print("Nothing in this JSON")
    else:
        print('Input data is ok')

    for item in input_json.items():
        print(item)

    print('now key values')

    for key, value in input_json.items():
        print(key)
        print(value)

    records = convert_json(input_json)

    with open(output_file, 'w') as outfile:
        json.dump(records, outfile)


# Define the Airflow DAG
with DAG(
        dag_id='pyspark_data_cleaning',
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
            "start_date": datetime(2023, 1, 1),
        },
        catchup=False,
) as dag:
    # Task to create the target HDFS directory
    create_hdfs_dir1 = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_track_data_dir_1',
        directory='/user/hadoop/spotify/track_data/raw/audio_features/',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    preprocess_task1 = PythonOperator(
        task_id='preprocess_track_ids',
        python_callable=preprocess_track_ids,
        op_kwargs={'input_file': '/home/airflow/airflow/dags/data/track_ids.json',
                   'output_file': '/home/airflow/airflow/dags/data/track_ids_processed.json'},
        dag=dag,
    )

    preprocess_task2 = PythonOperator(
        task_id='preprocess_audio_features',
        python_callable=preprocess_audio_features,
        op_kwargs={'input_file': '/home/airflow/airflow/dags/data/audio_features.json',
                   'output_file': '/home/airflow/airflow/dags/data/audio_features_processed.json'},
        dag=dag,
    )

    create_hdfs_dir2 = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_track_data_dir_2',
        directory='/user/hadoop/spotify/track_data/raw/track_ids/',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    # Task to upload the local audio_features.json to HDFS
    upload_af_to_hdfs = HdfsPutFileOperator(
        task_id='upload_audio_features_to_hdfs',
        local_file='/home/airflow/airflow/dags/data/audio_features_processed.json',
        remote_file='/user/hadoop/spotify/track_data/raw/audio_features/audio_features.json',
        hdfs_conn_id='hdfs',
    )

    # Task to upload the local audio_features.json to HDFS
    upload_ti_to_hdfs = HdfsPutFileOperator(
        task_id='upload_track_ids_to_hdfs',
        local_file='/home/airflow/airflow/dags/data/track_ids_processed.json',
        remote_file='/user/hadoop/spotify/track_data/raw/track_ids/track_ids.json',
        hdfs_conn_id='hdfs',
    )

    create_hdfs_dir3 = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_track_data_dir_3',
        directory='/user/hadoop/spotify/track_data/final/model_data/',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    # Task to clean and join data
    clean_data = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_data,
        op_args=['/user/hadoop/spotify/track_data/raw/'],
        dag=dag,
    )

    preprocess_task1 >> preprocess_task2 >> create_hdfs_dir1
    create_hdfs_dir1 >> create_hdfs_dir2 >> upload_ti_to_hdfs >> upload_af_to_hdfs
    upload_af_to_hdfs >> create_hdfs_dir3 >> clean_data
