# Apache Airflow, Spark, and HDFS Big Data Project

This repository contains code and configurations for a Big Data project implemented on Apache Airflow, Apache Spark, and Hadoop Distributed File System (HDFS). The project involves querying data from Spotify, storing it in HDFS, cleaning the data with PySpark, evaluating the data using a pre-trained machine learning model, saving the output to HDFS, building a Hive SQL table, and presenting the results in a simple frontend.

## Table of Contents
1. [Setup](#setup)
    - [Container Startup](#container-startup)
    - [Packages Installation](#packages-installation)
2. [Project Structure](#project-structure)
3. [Airflow DAGs](#airflow-dags)
    - [1. Data Crawling from Spotify](#1-data-crawling-from-spotify)
    - [2. PySpark Data Cleaning](#2-pyspark-data-cleaning)
    - [3. Model Prediction](#3-model-prediction)
    - [4. DB - Hive SQL](#4-db---hive-sql)
    - [5. HTML Frontend](#5-html-frontend)

---

## Setup

### Container Startup

1. **SSH to your VM:**
    ```bash
    ssh your_username@your_vm_ip
    ```

2. **Start Hadoop and Spark Containers:**
    ```bash
    docker pull marcelmittelstaedt/spark_base:latest
    docker start hadoop
    docker run -dit --name hadoop -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 10000:10000 -p 8032:8032 -p 8030:8030 -p 8031:8031 -p 9000:9000 -p 8888:8888 --net bigdatanet marcelmittelstaedt/spark_base:latest
    docker exec -it hadoop bash
    sudo su hadoop
    cd
    start-all.sh
    ```

3. **Forward Port 8080 for Airflow UI:**
    In another terminal, execute:
    ```bash
    ssh -L 8080:localhost:8080 your_username@your_vm_ip
    ```

4. **Start Airflow Container:**
    ```bash
    docker start airflow
    docker pull marcelmittelstaedt/airflow:latest
    docker run -dit --name airflow -p 8080:8080 -p 5000:5000 --net bigdatanet marcelmittelstaedt/airflow:latest
    docker exec -it airflow bash
    sudo su airflow
    cd
    ```

5. **Access Airflow UI:**
    Open [http://localhost:8080/admin/](http://localhost:8080/admin/) in your browser.

### Packages Installation

Install the required packages inside the Airflow container:

```bash
pip install Spotipy
```

# Project Structure
---

## Project Structure

```plaintext
/Home/Airflow/airflow
├── App
│   ├── __init__.py
│   ├── run_flask_app.py
│   └── Templates
│       └── table.html
└── Dags
    ├── Models
    │   └── genre_evaluator_pickle.pkl
    ├── config.yaml
    ├── query_functions.py
    ├── spotify_data_query.py
    ├── html_frontend_dag.py
    ├── model_prediction_spark.py
    ├── pyspark_data_clean.py
    └── hive_db_dag.py
```

---

## Airflow DAGs

### 1. Data Crawling from Spotify

- **File:** `airflow/dags/spotify_data_query.py`
- **Description:**
  - PythonOperator handling Spotify data querying.
  - Steps:
    1. Load Spotipy for user authentication and requests.
    2. Execute functions from `dags/query_functions.py`.
      - `Save_track_ids`: Crawls selected playlists for track IDs and titles.
      - `get_audio_features`: Queries track IDs for audio features.
    3. HDFS Operators:
      - Create a directory and put `audio_features.json` into `/user/hadoop/spotify/track_data/raw/{{ ds_nodash }}/audio_features.json`.

### 2. PySpark Data Cleaning

- **File:** `airflow/dags/pyspark_data_clean.py`
- **Description:**
  - Main job is to drop unnecessary columns and combine track data with audio_features data.
  - Steps:
    1. Preprocessing Python Operators to format `audio_features.json` and `track_ids.json`.
    2. HDFS Operators: Create a directory for track_ids and audio_features in `/user/hadoop/spotify/track_data/final/`.
    3. Pyspark Cleaning: Pyspark script drops unnecessary columns and combines both into one JSON. Put the JSON into the `hdfs/..final/model/` directory.

### 3. Model Prediction

- **File:** `airflow/dags/model_prediction_spark.py`
- **Description:**
  - Leverages a GitHub repository with a pre-trained machine learning model for calculating genres from audio_features: [music-genre_classif](https://github.com/navodas/music-genre_classif)
  - Steps:
    1. Transform the model to a Spark model using a RandomForestClassificationModel transform.
    2. Python Operator with a PySpark Script executes a model prediction based on the prepared data from HDFS.
    3. Reverse the label encoder to get actual genres.
    4. Outputs `music_and_genres.json` and `music_and_genres.csv` in `dag/data`.

### 4. DB - Hive SQL

- **File:** `airflow/dags/hive_db_dag.py`
- **Description:**
  - HDFS Operator: Puts `music_and_genres.json` and `music_and_genres.csv` in HDFS `final`.
  - HiveOperators:
    - Creates an external Table `music_and_genres` from the `music_and_genres.csv` file.
    - Debug Test Query of the DB.

### 5. HTML Frontend

- **File:** `airflow/dags/html_frontend_dag.py`
- **Description:**
  - PythonOperator runs a Flask app.
  - Steps:
    - `run_flask_app.py`: Creates a Flask App, queries the top 10 rows from the Database.
    - Uses a table from `app/templates/table.html`.
    - Sets the app to host at `0.0.0.0`, port `5000`, so the query can be seen outside a Docker container environment.

---

#### Credits:
- The machine learning model used in this project is from the GitHub repository: [music-genre_classif](https://github.com/navodas/music-genre_classif).
