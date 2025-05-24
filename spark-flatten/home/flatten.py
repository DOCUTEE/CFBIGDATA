from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import *

# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaToClickHouse") \
    .getOrCreate()

# 2. Schema JSON
schema = StructType([
    StructField("id", LongType()),
    StructField("relativeTimeSeconds", IntegerType()),
    StructField("programmingLanguage", StringType()),
    StructField("verdict", StringType()),
    StructField("passedTestCount", IntegerType()),
    StructField("timeConsumedMillis", IntegerType()),
    StructField("memoryConsumedBytes", LongType()),
    StructField("handle", StringType()),
    StructField("problem", StructType([
        StructField("contestId", IntegerType()),
        StructField("index", StringType()),
        StructField("name", StringType()),
        StructField("points", DoubleType()),
        StructField("rating", IntegerType()),
        StructField("tags", ArrayType(StringType()))
    ]))
])

# 3. Đọc từ Kafka
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "cleaned") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON
df_json = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Làm phẳng tags (explode)
df_flat = df_json.withColumn("problem_tag", explode(col("problem.tags")))

# 6. Chọn các cột cần thiết
df_result = df_flat.select(
    "id",
    "handle",
    "verdict",
    "programmingLanguage",
    "passedTestCount",
    "timeConsumedMillis",
    "memoryConsumedBytes",
    col("problem.contestId").alias("contestId"),
    col("problem.index").alias("problemIndex"),
    col("problem.name").alias("problemName"),
    col("problem.points").alias("problemPoints"),
    col("problem.rating").alias("problemRating"),
    col("problem_tag")
)

# 7. Gửi vào ClickHouse
df_result.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://cf-clickhouse:8123/cf") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "cf.submissions") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()
    ) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
