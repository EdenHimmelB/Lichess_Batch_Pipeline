from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
    ArrayType,
)
import argparse

spark: SparkSession = (
    SparkSession.builder.master("local[*]")
    .appName("CSV to Parquet Conversion")
    .getOrCreate()
)

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

moveSchema = ArrayType(
    StructType(
        [
            StructField("move", StringType(), True),
            StructField("eval", StringType(), True),
            StructField("time", StringType(), True),
        ]
    )
)


def transform_chess_data(csv_path, parquet_path) -> None:
    df = spark.read.csv(csv_path, header=True, schema=schema)
    df = df.withColumn(
        "UTCDateTime", F.concat(F.col("UTCDate"), F.lit(" "), F.col("UTCTime"))
    )

    df = df.select(
        F.col("GameID").alias("game_id"),
        F.date_format(
            F.to_timestamp(F.col("UTCDateTime"), "yyyy.MM.dd HH:mm:ss"),
            "HH:mm:ss dd-MM-yyyy",
        ).alias("timestamp"),
        F.trim(
            F.regexp_substr(str=F.col("Event"), regexp=F.lit("^(?:(?!https://).)*"))
        ).alias("game_type"),
        F.trim(F.regexp_substr(str=F.col("Event"), regexp=F.lit("(https://.+)"))).alias(
            "tournament_url"
        ),
        F.col("Site").alias("game_url"),
        F.col("TimeControl").alias("time_control"),
        F.col("White").alias("white_player"),
        F.col("Black").alias("black_player"),
        F.col("WhiteElo").alias("white_elo"),
        F.col("BlackElo").alias("black_elo"),
        F.col("WhiteRatingDiff").alias("white_rating_change"),
        F.col("BlackRatingDiff").alias("black_rating_change"),
        F.col("Result").alias("result"),
        F.col("Termination").alias("termination"),
        F.col("ECO").alias("eco"),
        F.col("Opening").alias("opening"),
        F.from_json(F.regexp_replace("GameMoves", "'", '"'), moveSchema).alias(
            "game_moves"
        ),
    )
    df.write.parquet(parquet_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert csv file and upload to cloud as parquet."
    )
    parser.add_argument(
        "--csv_path", type=str, help="name of the csv file which needs be converted"
    )
    args = parser.parse_args()
    csv_path = args.csv_path
    print(csv_path)
    parquet_path = csv_path.split(".")[0] + ".parquet"

    try:
        transform_chess_data(csv_path=csv_path, parquet_path=parquet_path)
    except Exception:
        print(Exception)

    spark.stop()
