import os, subprocess
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import storage

import requests

BASE_YEAR = datetime.now().strftime("%Y")
BASE_MONTH = (datetime.now() - relativedelta(months=1)).strftime("%m")
# BASE_URL = f"https://database.lichess.org/standard/lichess_db_standard_rated_{BASE_YEAR}-{BASE_MONTH}.pgn.zst"
BASE_URL = "https://storage.cloud.google.com/chess_object_storage_769413/standard_games/lichess_db_standard_rated_2024-01.pgn.zst"

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GOOGLE_BIGQUERY_DATASET = os.getenv("BIGQUERY_CHESS_DATASET")
GOOGLE_BIGQUERY_TABLE = os.getenv("BIGQUERY_CHESS_TABLE")
STORAGE_BUCKET_NAME = os.getenv("GOOGLE_STORAGE_BUCKET")

client = storage.Client()
bucket = client.bucket(STORAGE_BUCKET_NAME)


def download_data_to_gcs(url: str) -> str:
    uncompressed_file_name = url.split("/")[-1]
    blob = bucket.blob(uncompressed_file_name)

    # Stream the download and upload so that temp file isn't needed
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        blob.upload_from_file(
            r.raw, content_type=r.headers["Content-Type"], timeout=None
        )

    return f"gs://{STORAGE_BUCKET_NAME}/{uncompressed_file_name}"


def convert_raw_data_to_csv(uncompressed_file_path: str) -> str:
    converted_file_path = uncompressed_file_path.split(".")[0] + ".csv"
    subprocess.run(["python3", "-m", "pgn2csv", uncompressed_file_path])
    return converted_file_path


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    # "retries": 5,
    # "retry_delay": timedelta(days=1),
}

with DAG(
    dag_id="lichess_batch_pipeline",
    default_args=default_args,
    description="Download standard games from Lichess database, parse, populate DWH and other tools for downstream users",
    schedule_interval=None,
) as dag:

    with TaskGroup(group_id="ExtractLoad") as extract_load_tasks:
        download_task = PythonOperator(
            task_id="download_file",
            python_callable=download_data_to_gcs,
            provide_context=True,
            op_kwargs={"url": "https://storage.cloud.google.com/data_zoomcamp_mage_bucket_1/test_run.pgn.zst"},
        )

        preprocessing_task = PythonOperator(
            task_id="convert_pgn_zst_to_csv_format",
            python_callable=convert_raw_data_to_csv,
            op_kwargs={
                "uncompressed_file_path": "{{ ti.xcom_pull(task_ids='ExtractLoad.download_file') }}"
            },
            provide_context=True,
        )

        download_task >> preprocessing_task

    with TaskGroup(group_id="Transform") as transform_tasks:
        transform_task = SparkSubmitOperator(
            task_id="convert_and_upload_as_parquet_to_gcs",
            application="/opt/airflow/spark-jobs/transform_chess_batch_data.py",
            name="your_spark_job_name",
            conn_id="spark_default",
            application_args=[
                "--csv_path",
                "{{ ti.xcom_pull(task_ids='ExtractLoad.convert_pgn_zst_to_csv_format') }}",
            ],
            conf={
                "spark.jars": "/opt/airflow/spark-lib/gcs-connector-hadoop3-2.2.21-shaded.jar",
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "fs.gs.auth.service.account.enable": "true",
                "fs.gs.auth.service.account.json.keyfile": GOOGLE_CREDENTIALS,
            },
        )

    # with TaskGroup(group_id="ServeUpstream") as serve_tasks:
    #     load_bigquery_table = GCSToBigQueryOperator(
    #         task_id="gcs_to_bigquery",
    #         bucket=STORAGE_BUCKET_NAME,
    #         source_objects=["path/to/your/parquet/file-*.parquet"],
    #         destination_project_dataset_table=f"{GOOGLE_CLOUD_PROJECT}.{GOOGLE_BIGQUERY_DATASET}.{GOOGLE_BIGQUERY_TABLE}",
    #         source_format="PARQUET",
    #         write_disposition="WRITE_APPEND",
    #         create_disposition="CREATE_IF_NEEDED",
    #         # Create a partition on chess table based on the day from timetamp column
    #         time_partitioning={"type": "DAY", "field": "timestamp"},
    #     )

    extract_load_tasks >> transform_task
    # >> serve_tasks
