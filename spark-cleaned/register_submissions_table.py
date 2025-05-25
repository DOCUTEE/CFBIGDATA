from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterSubmissionsDeltaWithHive") \
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

# Register the Delta Lake table with Hive
spark.sql("""
CREATE TABLE IF NOT EXISTS submissions
USING DELTA
LOCATION 's3a://silver/submissions'
""")

# Confirm the table is visible in the metastore
spark.sql("SHOW TABLES").show()
