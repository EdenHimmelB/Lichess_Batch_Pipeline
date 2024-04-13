import os, subprocess, shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import storage

from pypdl import Downloader

BASE_YEAR = datetime.now().strftime("%Y")
BASE_MONTH = (datetime.now() - relativedelta(months=2)).strftime("%m")
BASE_URL = f"https://database.lichess.org/standard/lichess_db_standard_rated_{BASE_YEAR}-{BASE_MONTH}.pgn.zst"
# BASE_URL = (
#     "https://storage.googleapis.com/data_zoomcamp_mage_bucket_1/mock_data.pgn.zst"
# )

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BIGQUERY_CHESS_DATASET = os.getenv("BIGQUERY_CHESS_DATASET")
BIGQUERY_CHESS_TABLE = os.getenv("BIGQUERY_CHESS_TABLE")
STORAGE_BUCKET_NAME = os.getenv("GOOGLE_STORAGE_BUCKET")
RAW_FILE_NAME = BASE_URL.split("/")[-1]
CONVERTED_CSV_FILE_NAME = RAW_FILE_NAME.split(".")[0] + ".csv"
TABLE_SOURCE_FILE_URI = CONVERTED_CSV_FILE_NAME.split(".")[0] + "/*.parquet"

client = storage.Client()
bucket = client.bucket(STORAGE_BUCKET_NAME)

DATA_DIR_PATH = os.path.join(os.getcwd(), "data")


def download_data_to_local() -> str:
    try:
        os.mkdir(DATA_DIR_PATH)
    except FileExistsError:
        shutil.rmtree(DATA_DIR_PATH, ignore_errors=False, onerror=None)
        os.mkdir(DATA_DIR_PATH)

    local_raw_file_path = os.path.join(DATA_DIR_PATH, RAW_FILE_NAME)

    dl = Downloader(timeout=None)
    dl.start(
        url=BASE_URL,
        file_path=local_raw_file_path,
        segments=4,
        display=True,
        multithread=True,
        block=True,
        retries=0,
        mirror_func=None,
        etag=True,
    )
    return local_raw_file_path


def convert_raw_data_to_csv_locally(uncompressed_file_path: str) -> str:
    converted_csv_file_path = os.path.join(DATA_DIR_PATH, CONVERTED_CSV_FILE_NAME)
    subprocess.run(
        ["python3", "-m", "pgn2csv", uncompressed_file_path, converted_csv_file_path]
    )
    return converted_csv_file_path


def upload_csv_file_to_gcs(csv_file_path: str) -> str:
    csv_in_gcs_blob = bucket.blob(CONVERTED_CSV_FILE_NAME)
    with open(csv_file_path, "rb") as csv_stream:
        csv_in_gcs_blob.upload_from_file(csv_stream)
    return f"gs://{STORAGE_BUCKET_NAME}/{CONVERTED_CSV_FILE_NAME}"


def clean_up_local_env() -> None:
    shutil.rmtree(DATA_DIR_PATH, ignore_errors=False, onerror=None)


def load_parquet_to_bigquery(source_objects_uri, destination_project_dataset_table):
    hook = BigQueryHook()
    job_config = {
        "sourceFormat": "PARQUET",
        "sourceUris": [f"gs://{STORAGE_BUCKET_NAME}/{source_objects_uri}"],
        "destinationTable": {
            "projectId": destination_project_dataset_table.split(".")[0],
            "datasetId": destination_project_dataset_table.split(".")[1],
            "tableId": destination_project_dataset_table.split(".")[2],
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
        project_id=destination_project_dataset_table.split(".")[0],
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="lichess_batch_pipeline",
    default_args=default_args,
    description="Download standard games from Lichess database, parse, populate DWH and other tools for downstream users",
    schedule_interval=None,
) as dag:

    with TaskGroup(
        group_id="Extract_Preprocess_Upload"
    ) as extract_preprocess_upload_tasks:
        download_task = PythonOperator(
            task_id="download_data_to_local",
            python_callable=download_data_to_local,
            provide_context=True,
            op_kwargs={"url": BASE_URL},
        )

        preprocessing_task = PythonOperator(
            task_id="convert_pgn_zst_to_csv_format",
            python_callable=convert_raw_data_to_csv_locally,
            op_kwargs={
                "uncompressed_file_path": "{{ ti.xcom_pull(task_ids='Extract_Preprocess_Upload.download_data_to_local') }}"
            },
            provide_context=True,
        )

        upload_task = PythonOperator(
            task_id="upload_csv_to_gcs",
            python_callable=upload_csv_file_to_gcs,
            op_kwargs={
                "csv_file_path": "{{ ti.xcom_pull(task_ids='Extract_Preprocess_Upload.convert_pgn_zst_to_csv_format') }}",
            },
        )

        clean_up_task = PythonOperator(
            task_id="clean_up_local_env",
            python_callable=clean_up_local_env,
        )

        download_task >> preprocessing_task >> upload_task >> clean_up_task

    with TaskGroup(group_id="Transform") as transform_tasks:
        spark_transform_task = SparkSubmitOperator(
            task_id="convert_and_upload_as_parquet_to_gcs",
            application="/opt/airflow/spark-jobs/transform_chess_batch_data.py",
            name="your_spark_job_name",
            conn_id="spark_default",
            application_args=[
                "--input_path",
                "{{ ti.xcom_pull(task_ids='Extract_Preprocess_Upload.upload_csv_to_gcs') }}",
            ],
            conf={
                "spark.jars": "/opt/airflow/spark-lib/gcs-connector-hadoop3-2.2.21-shaded.jar",
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "fs.gs.auth.service.account.enable": "true",
                "fs.gs.auth.service.account.json.keyfile": GOOGLE_CREDENTIALS,
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.dynamicAllocation.enabled": "true",
                "spark.executor.cores": 8,
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
            },
        )
        spark_transform_task

    with TaskGroup(group_id="Load") as populate_dw_tasks:

        load_bigquery_table = PythonOperator(
            task_id="load_parquet_to_bigquery_custom",
            python_callable=load_parquet_to_bigquery,
            op_kwargs={
                # "bucket": "your-bucket-name",
                "source_objects_uri": TABLE_SOURCE_FILE_URI,
                "destination_project_dataset_table": f"{GOOGLE_CLOUD_PROJECT}.{BIGQUERY_CHESS_DATASET}.{BIGQUERY_CHESS_TABLE}",
            },
        )
        load_bigquery_table

    extract_preprocess_upload_tasks >> transform_tasks >> populate_dw_tasks
