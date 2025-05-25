from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, split, regexp_replace, expr, struct
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, ArrayType, StructField

# 1. Start SparkSession with Delta Lake + S3 support
spark = SparkSession.builder \
    .appName("KafkaToMinIO_Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
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
        StructField("tags", StringType())
    ])),
    StructField("handle", StringType())
])

# 3. Read streaming data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "cleaned") \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "spark-cleaned") \
    .load()

# 4. Parse Kafka value as JSON
df_json = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df_tags_array = df_json.withColumn(
    "tags_array",
    split(
        regexp_replace(col("problem.tags"), r"[\'\[\]]", ""),
        ",\\s*"
    )
).withColumn("tags_array", expr("transform(tags_array, x -> trim(x))"))

# Ghi đè `problem.tags` bằng array
df_parsed = df_tags_array.withColumn(
    "problem",
    struct(
        col("problem.contestId"),
        col("problem.index"),
        col("problem.name"),
        col("problem.points"),
        col("problem.rating"),
        col("tags_array").alias("tags")
    )
)

stop_flag = False

def process_batch(batch_df, batch_id):
    global stop_flag

    # Kiểm tra nếu có dòng với id = -1
    if batch_df.filter(col("id") == -1).count() > 0:
        print("Detected id = -1, initiating graceful stop after processing valid records...")
        stop_flag = True

    # Lọc chỉ các bản ghi hợp lệ (id != -1)
    valid_df = batch_df.filter(col("id") != -1)

    # Ghi dữ liệu hợp lệ vào Delta Lake
    if not valid_df.isEmpty():
        valid_df.write \
            .format("delta") \
            .mode("append") \
            .save("s3a://silver/submissions")
        print(f"Batch {batch_id}: Wrote {valid_df.count()} valid records to Delta Lake")
    else:
        print(f"Batch {batch_id}: No valid records to write to Delta Lake")

    # Nếu stop_flag được đặt, dừng StreamingContext
    if stop_flag:
        print("Stopping Spark Streaming gracefully...")
        query.stop()

# 5. Write to Delta Lake in MinIO S3 bucket
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://silver/checkpoints/kafka_to_minio") \
    .start("s3a://silver/submissions")

query.awaitTermination()
query.stop()


