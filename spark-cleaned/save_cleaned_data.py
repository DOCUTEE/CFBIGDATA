from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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
        StructField("tags", ArrayType(StringType()))
    ])),
    StructField("handle", StringType())
])

# 3. Read streaming data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "submissions") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse Kafka value as JSON
df_json = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Write to Delta Lake in MinIO S3 bucket
query = df_json.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://silver/checkpoints/kafka_to_minio") \
    .start("s3a://silver/submissions") \

query.awaitTermination()


