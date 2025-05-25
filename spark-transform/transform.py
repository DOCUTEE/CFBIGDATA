from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, concat_ws, explode
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, ArrayType, StructField
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# Step 1: Spark session with Delta + MinIO (S3A)
spark = SparkSession.builder \
    .appName("ReadTransformDeltaFromS3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Step 2: Read Delta table
df = spark.read.format("delta").load("s3a://silver/submissions")

# Step 3: Dimension tables
df_user = df.select("handle").distinct()
df_dim_user = df_user.select(
    col("handle").alias("user_handle"),
    sha2(col("handle"), 256).alias("user_id")
)

df_problem_raw = df.select(
    "problem.contestId", "problem.index", "problem.name", "problem.points", "problem.rating", "problem.tags"
).dropna()

df_problem_exploded = df_problem_raw.withColumn("problem_tag", explode("tags"))
df_problem_exploded = df_problem_exploded.withColumn("problem_id", sha2(concat_ws("_", "contestId", "index"), 256))
df_problem_exploded = df_problem_exploded.withColumn("tag_id", sha2(col("problem_tag"), 256))

df_dim_problem = df_problem_exploded.select(
    "problem_id", col("contestId").alias("contest_id"), col("index").alias("problem_index"), col("name").alias("problem_name"), "points", "rating"
).dropDuplicates()

df_dim_tag = df_problem_exploded.select(
    col("problem_tag").alias("tag_name"),
    "tag_id"
).dropDuplicates()

df_dim_problem_tag = df_problem_exploded.select("problem_id", "tag_id").dropDuplicates()

df_dim_verdict = df.select("verdict").dropna().distinct().select(
    col("verdict").alias("verdict_name"),
    sha2(col("verdict"), 256).alias("verdict_id")
)

df_dim_language = df.select("programmingLanguage").dropna().distinct().select(
    col("programmingLanguage").alias("language_name"),
    sha2(col("programmingLanguage"), 256).alias("language_id")
)

# Step 4: Fact table
df_fact_submission = df \
    .join(df_dim_user, df.handle == df_dim_user.user_handle, "inner") \
    .join(
        df_dim_problem,
        (df.problem.contestId == df_dim_problem.contest_id) &
        (df.problem.index == df_dim_problem.problem_index),
        "inner"
    ) \
    .join(df_dim_verdict, df.verdict == df_dim_verdict.verdict_name, "inner") \
    .join(df_dim_language, df.programmingLanguage == df_dim_language.language_name, "inner") \
    .select(
        col("id").alias("submission_id"),
        "user_id", "problem_id", "verdict_id", "language_id",
        col("relativeTimeSeconds").alias("relative_time_seconds"),
        "passedTestCount", "timeConsumedMillis", "memoryConsumedBytes"
    )

# Step 5: Define paths
delta_paths = {
    "dim_user": "s3a://gold/delta-dim/dim_user",
    "dim_problem": "s3a://gold/delta-dim/dim_problem",
    "dim_verdict": "s3a://gold/delta-dim/dim_verdict",
    "dim_tag": "s3a://gold/delta-dim/dim_tag",
    "dim_tag_problem": "s3a://gold/delta-dim/dim_tag_problem",
    "dim_language": "s3a://gold/delta-dim/dim_language",
    "fact_submission": "s3a://gold/delta-fact/fact_submission"
}

# Step 6: Save with merge (or create)
def save_delta_table(df: DataFrame, path: str, condition: str = None):
    if DeltaTable.isDeltaTable(spark, path) and condition:
        DeltaTable.forPath(spark, path) \
            .alias("target") \
            .merge(df.alias("source"), condition) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta").mode("overwrite").save(path)
    print(f"✅ Saved to: {path}")

# Step 7: Save all tables
save_delta_table(df_dim_user, delta_paths["dim_user"], "target.user_id = source.user_id")
save_delta_table(df_dim_problem, delta_paths["dim_problem"], "target.problem_id = source.problem_id")
save_delta_table(df_dim_verdict, delta_paths["dim_verdict"], "target.verdict_id = source.verdict_id")
save_delta_table(df_dim_tag, delta_paths["dim_tag"], "target.tag_id = source.tag_id")
save_delta_table(df_dim_problem_tag, delta_paths["dim_tag_problem"], "target.problem_id = source.problem_id AND target.tag_id = source.tag_id")
save_delta_table(df_dim_language, delta_paths["dim_language"], "target.language_id = source.language_id")
save_delta_table(df_fact_submission, delta_paths["fact_submission"], "target.submission_id = source.submission_id")

spark.stop()
