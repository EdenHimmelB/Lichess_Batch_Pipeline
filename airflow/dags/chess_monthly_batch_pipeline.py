import os
from datetime import timedelta, datetime

from pgn2csv import Runner

from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def load_raw_data_to_gcp_storage():
    pass

def parse_raw_data_to_csv():
    runner = Runner()


def partition_csv_file_to_parquet():
    pass
