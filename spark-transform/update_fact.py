from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, concat_ws, explode
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, ArrayType, StructField
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import os

# 1. Create SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("UpdateFact") \
        


df_static_user = spark.read.format("delta").load("/opt/spark-transform/delta-dim/dim_user")
df_static_problem = spark.read.format("delta").load("/opt/spark-transform/delta-dim/dim_problem")
df_static_verdict = spark.read.format("delta").load("/opt/spark-transform/delta-dim/dim_verdict")
df_static_language = spark.read.format("delta").load("/opt/spark-transform/delta-dim/dim_language")

fact_submission = df_json \
    .join(df_static_user, df_json.handle == df_static_user.handle, "left") \
    .join(df_static_problem, (df_json.problem.contestId == df_static_problem.contest_id) &
                (df_json.problem.index == df_static_problem.problem_index), "left") \
    .join(df_static_verdict, df_json.verdict == df_static_verdict.verdict, "left") \
    .join(df_static_language, df_json.programmingLanguage == df_static_language.programming_language, "left") \
    .select(
        df_json.id.alias("submission_id"),
        df_json.relativeTimeSeconds.alias("relative_time_seconds"),
        df_json.passedTestCount.alias("passed_test_count"),
        df_json.timeConsumedMillis.alias("time_consumed_millis"),
        df_json.memoryConsumedBytes.alias("memory_consumed_bytes"),
        df_static_user.user_id.alias("user_id"),
        df_static_problem.problem_id.alias("problem_id"),
        df_static_verdict.verdict_id.alias("verdict_id"),
        df_static_language.language_id.alias("language_id")
    )

# Define the path for the Delta table
fact_submission_path = "/opt/spark-transform/delta-fact/fact_submission"
def upsert_fact_submission(microbatch_df: DataFrame, batch_id: int):
    if DeltaTable.isDeltaTable(spark, fact_submission_path):
        delta_table = DeltaTable.forPath(spark, fact_submission_path)
        delta_table.alias("target").merge(
            microbatch_df.alias("source"),
            "target.submission_id = source.submission_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        microbatch_df.write.format("delta").mode("overwrite").save(fact_submission_path)
    print("OK7")

query_fact = fact_submission.writeStream \
    .foreachBatch(upsert_fact_submission) \
    .outputMode("update") \
    .option("checkpointLocation", "/opt/spark-transform/checkpoints/fact_submission") \
    .start()
