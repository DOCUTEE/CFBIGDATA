from pyspark.sql import SparkSession

clickhouse_url = "jdbc:clickhouse://cf-clickhouse:8123/lakehouse"
clickhouse_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

delta_paths = {
    "dim_user": "s3a://gold/delta-dim/dim_user",
    "dim_problem": "s3a://gold/delta-dim/dim_problem",
    "dim_verdict": "s3a://gold/delta-dim/dim_verdict",
    "dim_tag": "s3a://gold/delta-dim/dim_tag",
    "dim_tag_problem": "s3a://gold/delta-dim/dim_tag_problem",
    "dim_language": "s3a://gold/delta-dim/dim_language",
    "fact_submission": "s3a://gold/delta-fact/fact_submission"
}

spark = SparkSession.builder \
    .appName("DeltaToClickHouseOverwrite") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


def run_truncate_jdbc(table_name):
    sc = spark.sparkContext
    jvm = sc._jvm
    # Load driver class explicitly
    jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    DriverManager = jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(clickhouse_url, clickhouse_properties["user"], clickhouse_properties["password"])
    stmt = conn.createStatement()
    stmt.execute(f"TRUNCATE TABLE {table_name}")
    stmt.close()
    conn.close()
    print(f"Truncated table {table_name}")


for table, delta_path in delta_paths.items():
    print(f"Processing table: {table}")

    # Truncate table
    run_truncate_jdbc(table)

    # Load Delta data
    df = spark.read.format("delta").load(delta_path)

    # Rename columns if table is fact_submission to match ClickHouse snake_case schema
    if table == "fact_submission":
        df = df.withColumnRenamed("passedTestCount", "passed_test_count") \
               .withColumnRenamed("timeConsumedMillis", "time_consumed_millis") \
               .withColumnRenamed("memoryConsumedBytes", "memory_consumed_bytes") \
               .withColumnRenamed("relativeTimeSeconds", "relative_time_seconds")

    # Write to ClickHouse
    df.write.jdbc(
        url=clickhouse_url,
        table=table,
        mode="append",
        properties=clickhouse_properties
    )
    print(f"Inserted data into {table}")

spark.stop()
