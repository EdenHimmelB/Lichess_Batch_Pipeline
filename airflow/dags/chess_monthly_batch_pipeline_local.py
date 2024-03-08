import os, sys, re, subprocess
from datetime import timedelta, datetime

sys.path.append(os.getcwd())

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pyarrow  as pa
import pyarrow.parquet as pq

from pgn2csv import Runner


FILE_ID_PATTERN = re.compile(
    r"https:\/\/drive\.google\.com\/file\/d\/(.*?)\/view\?usp=(?:.*)"
)


def download_raw_data_from_gdrive_to_local(shareable_link):
    file_id = FILE_ID_PATTERN.match(shareable_link).group(1)
    downloadable_link = f"https://drive.google.com/uc?export=download&id={file_id}"

    p = subprocess.Popen(
        (
            "wget",
            "--no-check-certificate",
            "-O",
            "data/mock_data.pgn.zst",
            f"{downloadable_link}",
        ),
        stdout=subprocess.PIPE,
    )
    return p.wait()


# def parse_raw_data_to_csv():
#     runner = Runner()


def partition_csv_file_to_parquet():
    

download_raw_data_from_gdrive_to_local(
    "https://drive.google.com/file/d/1d7YF54Fij2yfXECZhQpF8jAB-98S99hi/view?usp=drive_link"
)
