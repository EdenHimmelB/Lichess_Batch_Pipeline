from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
import sys

schema = StructField([StringType()])


def convert_csv_to_parquet(csv_file_path, output_path):
    spark = (
        SparkSession.builder.appName("CSV to Parquet Conversion")
        .config("spark.jars", "gcs-connector-hadoop3-2.2.21-shaded.jar")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("fs.gs.auth.service.account.enable", "true")
        # .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/opt/spark/google_credentials.json")
        .config(
            "fs.gs.auth.service.account.json.keyfile",
            "./opt/spark/google_credentials.json",
        )
        .getOrCreate()
    )
    df = spark.read.csv(csv_file_path, header=True, schema=schema)
    df.write.parquet(output_path)
    spark.stop()


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    convert_csv_to_parquet(input_path, output_path)
