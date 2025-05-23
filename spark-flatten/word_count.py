from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, size, split, col, current_timestamp

spark = SparkSession.builder \
    .appName("KafkaWordCountToClickHouse") \
    .getOrCreate()

# Đọc từ Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "cf-broker:19092") \
    .option("subscribe", "sentences") \
    .load()

# Parse dữ liệu
parsed_df = df.selectExpr("CAST(value AS STRING) as sentence") \
    .withColumn("word_count", size(split(col("sentence"), " "))) \
    .withColumn("event_time", current_timestamp())

# Ghi ra ClickHouse (qua JDBC)
def write_to_clickhouse(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://cf-clickhouse:8123/test") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "word_count") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()

query = parsed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("update") \
    .start()

query.awaitTermination()