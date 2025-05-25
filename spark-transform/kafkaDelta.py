from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, concat_ws, explode
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, ArrayType, StructField
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import os

def create_spark_session(app_name: str = "KafkaToDeltaLake") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def define_schema() -> StructType:
    return StructType([
        StructField("id", IntegerType()),
        StructField("relativeTimeSeconds", IntegerType()),
        StructField("programmingLanguage", StringType()),
        StructField("verdict", StringType()),
        StructField("passedTestCount", IntegerType()),
        StructField("timeConsumedMillis", FloatType()),
        StructField("memoryConsumedBytes", FloatType()),
        StructField("problem", StructType([
            StructField("contestId", StringType()),
            StructField("index", StringType()),
            StructField("name", StringType()),
            StructField("points", FloatType()),
            StructField("rating", FloatType()),
            StructField("tags", ArrayType(StringType()))
        ])),
        StructField("handle", StringType())
    ])

def upsert_to_delta(df: DataFrame, batch_id: int, delta_path: str, merge_condition: str):
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)