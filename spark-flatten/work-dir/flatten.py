from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Khởi tạo Spark Session với các package cần thiết
spark = SparkSession.builder \
    .appName("KafkaToClickHouseStreaming") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu Kafka
schema = StructType([
    StructField("id", LongType(), False),
    StructField("relativeTimeSeconds", IntegerType(), True),
    StructField("programmingLanguage", StringType(), True),
    StructField("verdict", StringType(), True),
    StructField("passedTestCount", IntegerType(), True),
    StructField("timeConsumedMillis", IntegerType(), True),
    StructField("memoryConsumedBytes", LongType(), True),
    StructField("handle", StringType(), True),
    StructField("problem", StructType([
        StructField("contestId", IntegerType(), True),
        StructField("index", StringType(), True),
        StructField("name", StringType(), True),
        StructField("points", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("tags", StringType(), True)
    ]), True)
])

# Đọc stream từ Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "cleaned") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON từ Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Làm phẳng trường problem và xử lý tags
flattened_df = parsed_df.select(
    col("id"),
    (expr("timestamp_seconds(relativeTimeSeconds)")).alias("relativeTime"),
    col("programmingLanguage"),
    col("verdict"),
    col("passedTestCount"),
    col("timeConsumedMillis"),
    col("memoryConsumedBytes"),
    col("handle"),
    col("problem.contestId").alias("problem_contestId"),
    col("problem.index").alias("problem_index"),
    col("problem.name").alias("problem_name"),
    col("problem.points").alias("problem_points"),
    col("problem.rating").alias("problem_rating"),
    col("problem.tags").alias("tag")
    # Làm sạch tags: bỏ dấu [], ', và tách thành array
    # explode(
    #     split(
    #         regexp_replace(col("problem.tags"), r"[\[\]']", ""), 
    #         ", "
    #     )
    # ).alias("tag")
)

# Hàm để ghi vào ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    # Cấu hình JDBC cho ClickHouse
    clickhouse_jdbc_url = "jdbc:clickhouse://cf-clickhouse:8123/cf"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "root",
        "password": "root"
    }

    # Ghi dữ liệu vào ClickHouse
    batch_df.write \
        .format("jdbc") \
        .option("url", clickhouse_jdbc_url) \
        .option("dbtable", "cf.submissions") \
        .option("driver", clickhouse_properties["driver"]) \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .mode("append") \
        .save()

# Ghi stream vào ClickHouse
query = flattened_df \
    .writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .start()

# Chờ stream kết thúc
query.awaitTermination()