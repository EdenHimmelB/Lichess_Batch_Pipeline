from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="test_spark_job",
    default_args=default_args,
    # description="Download standard games from Lichess database, process, populate DWH and other tools for downstream users",
    schedule_interval=None,  # Run this DAG on-demand
    catchup=False,
) as dag:

    upload_to_gcs_as_parquet_task = SparkSubmitOperator(
        task_id="convert_and_upload_as_parquet_to_gcs",
        application="/opt/airflow/spark-jobs/convert_csv_to_parquet.py",  # Path to your Spark application
        name="your_spark_job_name",
        conn_id="spark_default",  # The connection ID for your Spark cluster
        conf={
            "spark.jars": "/opt/airflow/spark-lib/gcs-connector-hadoop3-2.2.21-shaded.jar",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "fs.gs.auth.service.account.enable": "true",
            "fs.gs.auth.service.account.json.keyfile": credentials,
            # Add any additional Spark configurations here
        },
        # Include other parameters as needed
    )
