from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SampleDeltaWithHive") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Create a sample DataFrame
data = [("alice", 1), ("bob", 2)]
df = spark.createDataFrame(data, ["name", "id"])

# 2. Save as Delta table to MinIO
delta_path = "s3a://silver/sample_table"
df.write.format("delta").mode("overwrite").save(delta_path)

# 3. Register the Delta table with Hive
spark.sql(f"""
CREATE TABLE IF NOT EXISTS sample_table
USING DELTA
LOCATION '{delta_path}'
""")

# 4. Confirm the table is visible in the metastore
spark.sql("SHOW TABLES").show()