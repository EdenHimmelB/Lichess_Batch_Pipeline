import os
import subprocess
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from pypdl import Downloader

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

BASE_YEAR = datetime.now().strftime("%Y")
BASE_MONTH = (datetime.now() - relativedelta(months=1)).strftime("%m")
BASE_URL = "https://storage.googleapis.com/chess_raw_data_2024/chess_rated_games_partial.pgn.zst"

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BIGQUERY_CHESS_DATASET = os.getenv("BIGQUERY_CHESS_DATASET")
BIGQUERY_CHESS_TABLE = os.getenv("BIGQUERY_CHESS_TABLE")
STORAGE_BUCKET_NAME = os.getenv("GOOGLE_STORAGE_BUCKET")

RAW_FILE_NAME = BASE_URL.split("/")[-1]
CONVERTED_CSV_FILE_NAME = RAW_FILE_NAME.split(".")[0] + ".csv"
PARQUET_FOLDER_NAME = RAW_FILE_NAME.split(".")[0]
TABLE_SOURCE_FILE_URI = CONVERTED_CSV_FILE_NAME.split(".")[0] + "/*.parquet"

LOCAL_DATA_DIR_PATH = os.path.join(os.getcwd(), "data")
LOCAL_RAW_FILE_PATH = os.path.join(LOCAL_DATA_DIR_PATH, RAW_FILE_NAME)
LOCAL_CONVERTED_CSV_FILE_PATH = os.path.join(
    LOCAL_DATA_DIR_PATH, CONVERTED_CSV_FILE_NAME
)
CLOUD_PARQUET_FOLDER_URI = f"gs://{STORAGE_BUCKET_NAME}/{PARQUET_FOLDER_NAME}"


def download_data_to_local() -> None:
    dl = Downloader(timeout=None)
    dl.start(
        url=BASE_URL,
        file_path=LOCAL_RAW_FILE_PATH,
        segments=4,
        display=True,
        multithread=True,
        block=True,
        retries=0,
        mirror_func=None,
        etag=True,
    )


def convert_pgn_zst_to_csv_format() -> None:
    subprocess.run(
        ["python3", "-m", "pgn2csv", LOCAL_RAW_FILE_PATH, LOCAL_CONVERTED_CSV_FILE_PATH]
    )


def load_parquet_to_bigquery() -> None:
    hook = BigQueryHook()
    job_config = {
        "sourceFormat": "PARQUET",
        "sourceUris": [f"gs://{STORAGE_BUCKET_NAME}/{TABLE_SOURCE_FILE_URI}"],
        "destinationTable": {
            "projectId": GOOGLE_CLOUD_PROJECT,
            "datasetId": BIGQUERY_CHESS_DATASET,
            "tableId": BIGQUERY_CHESS_TABLE,
        },
        "timePartitioning": {"type": "DAY", "field": "timestamp"},
        "writeDisposition": "WRITE_APPEND",
        "createDisposition": "CREATE_IF_NEEDED",
        "parquetOptions": {
            "enableListInference": True,
        },
    }
    hook.insert_job(
        configuration={"load": job_config},
        project_id=GOOGLE_CLOUD_PROJECT,
    )


def clean_up_local_env() -> None:
    os.remove(LOCAL_CONVERTED_CSV_FILE_PATH)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 7,
    "retry_delay": timedelta(days=1),
}

with DAG(
    dag_id="mock_lichess_batch_pipeline",
    default_args=default_args,
    description="Download standard games from Lichess database, parse, populate DWH and other tools for downstream users",
    schedule_interval="0 8 1 * *",  # Run at 08:00 every month at day 1
) as dag:

    with TaskGroup(group_id="Extract_Preprocess") as extract_preprocess_tasks:
        download_task = PythonOperator(
            task_id="download_data_to_local",
            python_callable=download_data_to_local,
        )

        preprocessing_task = PythonOperator(
            task_id="convert_pgn_zst_to_csv_format",
            python_callable=convert_pgn_zst_to_csv_format,
        )

        download_task >> preprocessing_task

    with TaskGroup(group_id="Transform") as transform_tasks:
        spark_transform_task = SparkSubmitOperator(
            task_id="convert_and_upload_as_parquet_to_gcs",
            application="/opt/airflow/spark-jobs/transform_chess_batch_data.py",
            name="your_spark_job_name",
            conn_id="spark_default",
            application_args=[
                "--input_path",
                LOCAL_CONVERTED_CSV_FILE_PATH,
                "--output_path",
                CLOUD_PARQUET_FOLDER_URI,
            ],
            conf={
                "spark.jars": "/opt/airflow/spark-lib/gcs-connector-hadoop3-2.2.21-shaded.jar",
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "fs.gs.auth.service.account.enable": "true",
                "fs.gs.auth.service.account.json.keyfile": GOOGLE_CREDENTIALS,
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.dynamicAllocation.enabled": "true",
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
            },
        )
        spark_transform_task

    with TaskGroup(group_id="Load") as populate_dw_tasks:

        load_bigquery_table = PythonOperator(
            task_id="load_parquet_to_bigquery_custom",
            python_callable=load_parquet_to_bigquery,
        )
        load_bigquery_table

    clean_up_task = PythonOperator(
        task_id="clean_up_local_env",
        python_callable=clean_up_local_env,
    )

    extract_preprocess_tasks >> transform_tasks >> populate_dw_tasks >> clean_up_task
