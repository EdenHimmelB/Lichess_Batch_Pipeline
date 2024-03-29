import os, sys, subprocess

sys.path.append(os.getcwd())

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import storage

import requests

client = storage.Client()
bucket = client.bucket("data_zoomcamp_mage_bucket_1")


def download_data_to_gcs(url: str) -> str:
    # uncompressed_file_name = url.split("/")[-1]
    uncompressed_file_name = "test_run.pgn.zst"
    blob = bucket.blob(uncompressed_file_name)

    # Stream the download and upload so that temp file isn't needed
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        blob.upload_from_file(r.raw, content_type=r.headers["Content-Type"])

    return f"gcs://data_zoomcamp_mage_bucket_1/{uncompressed_file_name}"


def convert_raw_data_to_csv(uncompressed_file_path: str) -> str:
    converted_file_path = uncompressed_file_path.split(".")[0] + ".csv"
    subprocess.run(["python3", "-m", "pgn2csv", uncompressed_file_path])
    return converted_file_path

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="lichess_batch_pipeline",
    default_args=default_args,
    description="Download standard games from Lichess database, parse, populate DWH and other tools for downstream users",
    schedule_interval=None,  # Run this DAG on-demand
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_file",
        python_callable=download_data_to_gcs,
        op_kwargs={
            "url": "https://drive.google.com/uc?export=download&id=1d7YF54Fij2yfXECZhQpF8jAB-98S99hi&filename=small.pgn.zst"
        },
        provide_context=True,
    )

    conversion_task = PythonOperator(
        task_id="convert_pgn_zst_to_csv_format",
        python_callable=convert_raw_data_to_csv,
        op_kwargs={
            "uncompressed_file_path": "{{ ti.xcom_pull(task_ids='download_file') }}"
        },
        provide_context=True,
    )

    # upload_to_gcs_as_parquet_task = SparkSubmitOperator(
    #     task_id="convert_and_upload_as_parquet_to_gcs",
    # )

    download_task >> conversion_task 
    # >> upload_to_gcs_as_parquet_task
