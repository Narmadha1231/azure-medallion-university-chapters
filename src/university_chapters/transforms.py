from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

FEATURE_SCHEMA = StructType([
    StructField("attributes", StructType([
        StructField("OBJECTID", LongType(), True),
        StructField("ChapterID", StringType(), True),
        StructField("University_Chapter", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
    ]), True),
    StructField("geometry", StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
    ]), True),
])


def features_to_df(spark: SparkSession, features: list[dict], run_id: str) -> DataFrame:
    return spark.createDataFrame(features, FEATURE_SCHEMA).select(
        F.col("attributes.ChapterID").alias("chapter_id"),
        F.col("attributes.University_Chapter").alias("chapter_name"),
        F.col("attributes.City").alias("city"),
        F.upper(F.trim(F.col("attributes.State"))).alias("state"),
        F.col("geometry.x").cast("double").alias("longitude"),
        F.col("geometry.y").cast("double").alias("latitude"),
        F.col("attributes.OBJECTID").alias("source_object_id"),
        F.lit(run_id).alias("ingest_run_id"),
        F.current_timestamp().alias("processed_at_utc"),
        F.to_json(F.struct("attributes", "geometry")).alias("raw_payload"),
    ).filter(F.col("state").isin("CA", "OR", "WA"))


def apply_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid_coordinates = (
        F.col("longitude").isNull() | F.col("latitude").isNull() |
        ~F.col("longitude").between(-180.0, 180.0) |
        ~F.col("latitude").between(-90.0, 90.0)
    )
    quarantine = df.filter(invalid_coordinates).select(
        "*", F.lit("INVALID_COORDINATES").alias("quarantine_reason")
    )
    city_warning = (
        F.col("city").isNull() |
        (F.trim(F.col("city")) == "") |
        (F.upper(F.trim(F.col("city"))) == "UNKNOWN")
    )
    silver = df.filter(~invalid_coordinates).withColumn(
        "dq_status", F.when(city_warning, F.lit("WARNING")).otherwise(F.lit("OK"))
    ).withColumn(
        "dq_warnings",
        F.when(city_warning, F.array(F.lit("MISSING_OR_UNKNOWN_CITY")))
         .otherwise(F.array().cast(ArrayType(StringType())))
    ).drop("raw_payload")
    window = Window.partitionBy("chapter_id").orderBy(F.col("source_object_id").desc_nulls_last())
    silver = silver.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
    return silver, quarantine


def build_gold(silver: DataFrame) -> DataFrame:
    return silver.select(
        "chapter_id", "chapter_name", "city", "state", "longitude", "latitude",
        "dq_status", "dq_warnings", "ingest_run_id", "processed_at_utc"
    )
