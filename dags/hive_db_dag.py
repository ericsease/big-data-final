from datetime import datetime, timedelta

import findspark
from airflow import DAG
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession


# Function to convert JSON to Parquet
def json_to_parquet(json_file_path):
    print("We're in the json_to_parquet function")
    # Retrieve HDFS path from the context
    # Construct Parquet output path by replacing '.json' with '.parquet'
    parquet_output_path = json_file_path.replace('.json', '.parquet')
    textfile_output_path = json_file_path.replace('.json', '.txt')

    # Print paths for debugging
    print(f"Input JSON path: {json_file_path}")
    print(f"Output Parquet path: {parquet_output_path}")

    findspark.init()
    findspark.find()
    # Initialize Spark session
    spark = SparkSession.builder.appName("json_to_parquet").getOrCreate()

    # Load JSON data into a DataFrame
    df = spark.read.json(json_file_path)
    df.show()

    # Try txt
    df.write.option("header", "false").option("delimiter", "\t").csv(textfile_output_path,
                                                                     sep="\t",
                                                                     mode="overwrite")

    # Save DataFrame as Parquet
    df.write.parquet(parquet_output_path, mode="overwrite")
    print("Reached the end of the file.")

    # Stop the Spark session
    spark.stop()


# Define the Hive query to create the table
hiveSQL_create_music_and_genres_table = '''
    
    DROP TABLE IF EXISTS music_and_genres;
    
    CREATE EXTERNAL TABLE IF NOT EXISTS music_and_genres (
        id STRING,
        title STRING,
        genre STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hadoop/spotify/track_data/final/csv';
    '''

hiveSQL_query_music_and_genres = '''
SELECT *
FROM music_and_genres;
'''

# Define the Airflow DAG
with DAG(
        dag_id='hive_db',
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
    create_hdfs_dir = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_track_data_dir',
        directory='/user/hadoop/spotify/track_data/final/',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    upload_to_hdfs = HdfsPutFileOperator(
        task_id='upload_audio_features_to_hdfs',
        local_file='/home/airflow/airflow/dags/data/music_and_genres.json',
        remote_file='/user/hadoop/spotify/track_data/final/music_and_genres.json',
        hdfs_conn_id='hdfs',
    )

    upload_textfile_to_hdfs = HdfsPutFileOperator(
        task_id='upload_genredata_to_hdfs',
        local_file='/home/airflow/airflow/dags/data/music_and_genres.csv',
        remote_file='/user/hadoop/spotify/track_data/final/csv/music_and_genres.csv',
        hdfs_conn_id='hdfs',
    )

    # PySpark JSON to Parquet conversion task
    json_to_parquet_task = PythonOperator(
        task_id="json_to_parquet_task",
        python_callable=json_to_parquet,
        op_args=['/user/hadoop/spotify/track_data/final/music_and_genres.json'],
        dag=dag,
    )

    # Create a HiveOperator for table creation
    create_music_and_genres_table_task = HiveOperator(
        task_id='create_music_and_genres_table',
        hql=hiveSQL_create_music_and_genres_table,
        hive_cli_conn_id='beeline',
        dag=dag,
    )

    # Add a new Hive query task
    hive_query_music_and_genres = HiveOperator(
        task_id='hive_query_music_and_genres',
        hql='SELECT * FROM music_and_genres LIMIT 10',
        hive_cli_conn_id='beeline',
        dag=dag,
    )


    # Set task dependencies
    create_hdfs_dir >> upload_to_hdfs >> upload_textfile_to_hdfs >> json_to_parquet_task
    json_to_parquet_task >> create_music_and_genres_table_task >> hive_query_music_and_genres
    hive_query_music_and_genres
