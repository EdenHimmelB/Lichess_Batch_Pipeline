from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
import sys

schema = StructType(
    [
        StructField("GameID", StringType(), False),
        StructField("Event", StringType(), True),
        StructField("Site", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Round", StringType(), True),
        StructField("White", StringType(), True),
        StructField("Black", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("UTCDate", StringType(), True),
        StructField("UTCTime", StringType(), True),
        StructField("WhiteElo", IntegerType(), True),
        StructField("BlackElo", IntegerType(), True),
        StructField("WhiteRatingDiff", FloatType(), True),
        StructField("BlackRatingDiff", FloatType(), True),
        StructField("WhiteTitle", StringType(), True),
        StructField("BlackTitle", StringType(), True),
        StructField("ECO", StringType(), True),
        StructField("Opening", StringType(), True),
        StructField("TimeControl", StringType(), True),
        StructField("Termination", StringType(), True),
        StructField("GameMoves", StringType(), True),
    ]
)


def convert_csv_to_parquet():
    spark: SparkSession = (
        SparkSession.builder.master("local[*]")
        .appName("CSV to Parquet Conversion")
        .getOrCreate()
    )
    df = spark.read.csv(
        "gs://data_zoomcamp_mage_bucket_1/mock_data.csv", header=True, schema=schema
    )
    df.write.parquet("gs://data_zoomcamp_mage_bucket_1/mock_parquet_folder")
    spark.stop()


if __name__ == "__main__":
    convert_csv_to_parquet()
