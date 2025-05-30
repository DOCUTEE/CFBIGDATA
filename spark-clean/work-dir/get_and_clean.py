from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, ArrayType
from kafka import KafkaProducer
import json

# Schema cho members (mảng chứa dict với trường handle)
members_schema = ArrayType(StructType([
    StructField("handle", StringType())
]))

# Schema ví dụ cho submission với các trường flattened (author.members là string JSON)
submission_schema = StructType([
    StructField("id", LongType()),
    StructField("contestId", IntegerType()),
    StructField("creationTimeSeconds", LongType()),
    StructField("relativeTimeSeconds", LongType()),
    StructField("programmingLanguage", StringType()),
    StructField("verdict", StringType()),
    StructField("testset", StringType()),
    StructField("passedTestCount", IntegerType()),
    StructField("timeConsumedMillis", LongType()),
    StructField("memoryConsumedBytes", LongType()),
    StructField("problem.contestId", IntegerType()),
    StructField("problem.index", StringType()),
    StructField("problem.name", StringType()),
    StructField("problem.points", IntegerType()),
    StructField("problem.rating", IntegerType()),
    StructField("problem.tags", StringType()),  # Tags là chuỗi JSON, cần parse sau
    StructField("author.contestId", IntegerType()),
    StructField("author.participantId", IntegerType()),
    StructField("author.members", StringType()),  # JSON string cần parse
    StructField("author.participantType", StringType()),
    StructField("author.ghost", BooleanType()),
    StructField("author.startTimeSeconds", LongType()),
    StructField("author.teamId", StringType()),
    StructField("author.teamName", StringType()),
    StructField("author.room", StringType())
])

def clean_submission(df):
    return df.select(
        col("id"),
        col("relativeTimeSeconds"),
        col("programmingLanguage"),
        col("verdict"),
        col("passedTestCount"),
        col("timeConsumedMillis"),
        col("memoryConsumedBytes"),
        col("members_array")[0]["handle"].alias("handle"),
        struct(
            col("`problem.contestId`").alias("contestId"),
            col("`problem.index`").alias("index"),
            col("`problem.name`").alias("name"),
            col("`problem.points`").alias("points"),
            col("`problem.rating`").alias("rating"),
            col("`problem.tags`").alias("tags")
        ).alias("problem")
    )
 


spark = SparkSession.builder \
    .appName("CleanSubmissionAndSendToKafka") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "submissions") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), submission_schema)) \
    .select("data.*")   

# Đổi tên cột author.members để tránh lỗi khi sử dụng dấu chấm
json_df = json_df.withColumnRenamed("author.members", "author_members")

# Parse JSON string trong author_members thành mảng struct
json_df = json_df.withColumn("members_array", from_json(col("author_members"), members_schema))

cleaned_df = clean_submission(json_df)

stop_flag = False

def send_to_cleaned(batch_df, batch_id):
    global stop_flag
    producer = KafkaProducer(
        bootstrap_servers='cf-broker:19092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Kiểm tra nếu có dòng với id = -1
    if batch_df.filter(col("id") == -1).count() > 0:
        print("Detected id = -1, initiating graceful stop...")
        stop_flag = True
    
    
    # Gửi dữ liệu hợp lệ tới Kafka topic "cleaned"
    for record in batch_df.toJSON().collect():
        producer.send("cleaned", value=json.loads(record))

    producer.flush()
    producer.close()

    # Nếu stop_flag được đặt, dừng StreamingContext
    if stop_flag:
        print("Stopping Spark Streaming gracefully...")
        query.stop()

query = cleaned_df.writeStream \
    .foreachBatch(send_to_cleaned) \
    .outputMode("update") \
    .start()

query.awaitTermination()
query.stop()
