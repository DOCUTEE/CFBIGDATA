from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, ArrayType, StructField

# 1. SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("KafkaToDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Kafka Stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "submissions") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Define schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("relativeTimeSeconds", IntegerType()),
    StructField("programmingLanguage", StringType()),
    StructField("verdict", StringType()),
    StructField("passedTestCount", IntegerType()),
    StructField("timeConsumedMillis", IntegerType()),
    StructField("memoryConsumedBytes", IntegerType()),
    StructField("problem", StructType([
        StructField("contestId", IntegerType()),
        StructField("index", StringType()),
        StructField("name", StringType()),
        StructField("points", FloatType()),
        StructField("rating", IntegerType()),
        StructField("tags", ArrayType(StringType()))
    ])),
    StructField("handle", StringType())
])

# 4. Parse JSON
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*", "data.problem.*").drop("problem")

# 5. Write to Delta Lake
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-transform/checkpoints/kafka-delta") \
    .start("/opt/spark-transform/delta-data")  # <= Change to /tmp for permission safety

query.awaitTermination()
