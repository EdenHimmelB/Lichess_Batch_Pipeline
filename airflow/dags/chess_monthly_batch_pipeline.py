import os, sys, subprocess

sys.path.append(os.getcwd())

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pyarrow.csv as pacsv
import pyarrow.parquet as papq
from pyarrow.fs import GcsFileSystem

import requests
from tqdm import tqdm


def download_raw_data_to_local(url: str) -> str:
    # uncompressed_file_name = url.split("/")[-1]
    uncompressed_file_name = "test_run.pgn.zst"

    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        # Retrieve total file size for progress reporting
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        block_size = 1024

        progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
        with open(uncompressed_file_name, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")

    # Push uncompressed file path to the next conversion step
    return uncompressed_file_name


def convert_raw_data_to_csv(uncompressed_file_path: str) -> str:
    parsed_file_path = uncompressed_file_path.split(".")[0] + ".csv"

    print(os.listdir())
    subprocess.run(["python3", "-m", "pgn2csv", uncompressed_file_path])

    return parsed_file_path


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
        op_kwargs={
            "url": "https://drive.google.com/uc?export=download&id=1d7YF54Fij2yfXECZhQpF8jAB-98S99hi&filename=small.pgn.zst"
        },
        provide_context=True,
    )

    parse_task = PythonOperator(
        task_id="convert_pgn_zst_to_csv_format",
        python_callable=convert_raw_data_to_csv,
        op_kwargs={
            "uncompressed_file_path": "{{ ti.xcom_pull(task_ids='download_file') }}"
        },
        provide_context=True,
    )

    # upload_to_gcs_as_parquet_task = PythonOperator(
    #     task_id="upload_to_gcs",
    #     python_callable=partition_csv_file_to_parquet,
    #     op_kwargs={},
    # )

    download_task >> parse_task
    # >> upload_to_gcs_as_parquet_task
