import os, sys

sys.path.append(os.getcwd())

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as papq
from pyarrow.fs import GcsFileSystem

import requests
from tqdm import tqdm

from pgn2csv import Runner


def download_raw_data_to_local(url, filename):
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        # Retrieve total file size for progress reporting
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        block_size = 1024

        progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
        with open(filename, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")


def parse_raw_data_to_csv(input_path: str, output_path: str) -> None:
    Runner.work(input_path, output_path)


def partition_csv_file_to_parquet(
    csv_file_path: str, parquet_output_path: str, partition_column: str
) -> None:
    gcs = GcsFileSystem()
    table = pacsv.read_csv(csv_file_path)
    papq.write_to_dataset(
        table=table,
        root_path=parquet_output_path,
        partition_cols=[partition_column],
        filesystem=gcs,
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
    schedule_interval=None,  # Run this DAG on-demand
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_file",
        python_callable=download_raw_data_to_local,
        provide_context=True,
    )

    parse_task = PythonOperator(
        task_id="parse pgn to csv format",
        python_callable=parse_raw_data_to_csv,
        
    )

    upload_to_gcs_as_parquet_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=partition_csv_file_to_parquet
    )

    download_task >> parse_task >> upload_to_gcs_as_parquet_task
