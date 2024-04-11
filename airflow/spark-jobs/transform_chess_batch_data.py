from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
    ArrayType,
    TimestampType,
)
import argparse

spark: SparkSession = (
    SparkSession.builder.master("local[8]")
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


def transform_chess_data(input_path, output_path) -> None:
    df = spark.read.csv(input_path, header=True, schema=schema)
    df = df.withColumn(
        "UTCDateTime", F.concat(F.col("UTCDate"), F.lit(" "), F.col("UTCTime"))
    )

    df = df.select(
        F.col("GameID").alias("game_id"),
        F.to_timestamp(F.col("UTCDateTime"), "yyyy.MM.dd HH:mm:ss").alias("timestamp"),
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
        F.when(F.col("ECO") == "?", F.lit(None)).otherwise(F.col("ECO")).alias("eco"),
        F.when(F.col("Opening") == "?", F.lit(None))
        .otherwise(F.col("Opening"))
        .alias("opening"),
        F.from_json(F.regexp_replace("GameMoves", "'", '"'), moveSchema).alias(
            "game_moves"
        ),
    )
    df.write.mode("overwrite").format("parquet").save(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File conversion")
    parser.add_argument(
        "--input_path", type=str, help="name of the csv file which needs be converted"
    )
    args = parser.parse_args()
    input_path = args.input_path
    output_path = input_path.split(".")[0]
    transform_chess_data(input_path=input_path, output_path=output_path)

    spark.stop()
