import json
import pickle
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def transform_scikit_to_spark_model():
    # Load scikit-learn model from pickle file
    with open('/home/airflow/airflow/dags/models/genre_evaluator_pickle.pkl', 'rb') as file:
        scikit_model = pickle.load(file)

    # Transform scikit-learn model to PySpark model
    spark_model = RandomForestClassificationModel.fromSKLearn(scikit_model)

    # Save the PySpark model
    spark_model.save('/home/airflow/airflow/dags/models/spark_model')


def add_predicted_genre_column(df, y_pred, mapping):
    df['predicted_genre'] = [mapping[pred] for pred in y_pred]
    return df


def predict_genre_from_spark_model():
    # Initialize Spark session
    spark = SparkSession.builder.appName("GenrePrediction").getOrCreate()

    # Load the PySpark model
    loaded_model = PipelineModel.load('/home/airflow/airflow/dags/models/spark_model')

    # Load audio features from JSON file
    audio_features_df = spark.read.json(
        'hdfs:///user/hadoop/spotify/track_data/final/model_data/cleaned_combined_data.json')

    # Convert numeric columns to float
    numeric_columns = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
                       'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
                       'duration_ms', 'time_signature']

    audio_features_df = audio_features_df.withColumn('popularity', 43)

    # Predict using the loaded Spark ML model
    predictions = loaded_model.transform(audio_features_df)

    # Extract the predicted genre column
    y_pred = predictions.select('prediction').alias('predicted_genre')

    # Assuming 'y_pred' contains your predicted values
    predictions = predictions.withColumn('predicted_genre', y_pred)

    # Display the DataFrame with the predicted genre
    predictions.select('predicted_genre').show()

    # Define the mapping
    mapping = {
        0: 'blues',
        1: 'classical',
        2: 'country',
        3: 'edm',
        4: 'hip hop',
        5: 'jazz',
        6: 'metal',
        7: 'pop',
        8: 'r&b',
        9: 'rock'
    }

    # Call the function to add the 'predicted_genre' column
    audio_features_df = add_predicted_genre_column(audio_features_df, y_pred, mapping)

    with open('./data/track_ids.json', 'r') as file:
        track_ids_data = json.load(file)

    # Convert the loaded JSON data to a PySpark DataFrame
    schema = StructType(
        [StructField("id", StringType(), True), StructField("title", StringType(), True)])
    title_df = spark.createDataFrame([(item["id"], item["title"]) for item in track_ids_data],
                                     schema=schema)

    # Merge/join the DataFrames on the "id" column
    result_df = audio_features_df.join(title_df, on="id", how="inner")

    # Select and reorder columns
    result_df = result_df.select("id", "title", "predicted_genre")

    # Rename predicted_genre to genre
    result_df = result_df.withColumnRenamed("predicted_genre", "genre")

    # Write the resulting DataFrame to JSON and CSV files
    result_df.write.json("./data/music_and_genres.json", mode='overwrite')
    result_df.write.csv("./data/music_and_genres.csv", header=False, mode='overwrite')

    # Stop the Spark session
    spark.stop()

    # Stop the Spark session
    spark.stop()


# Define default_args and DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
        'genre_prediction_dag',
        default_args=default_args,
        description='DAG for genre prediction using PySpark',
        schedule_interval=None,  # Set your desired schedule interval
) as dag:
    # Define tasks
    transform_task = PythonOperator(
        task_id='transform_scikit_to_spark_model',
        python_callable=transform_scikit_to_spark_model,
        dag=dag,
    )

    predict_task = PythonOperator(
        task_id='predict_genre_from_spark_model',
        python_callable=predict_genre_from_spark_model,
        dag=dag,
    )

    # Define task dependencies
    transform_task >> predict_task
