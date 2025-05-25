import os
import polars as pl
from deltalake import DeltaTable

# Set MinIO credentials
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9049"
os.environ["AWS_ALLOW_HTTP"] = "true"

# Define table path
table_paths = ["s3://gold/delta-dim/dim_user"
                , "s3://gold/delta-dim/dim_problem"
                , "s3://gold/delta-dim/dim_verdict"
                , "s3://gold/delta-dim/dim_language"
                , "s3://gold/delta-fact/fact_submission"
                , "s3://gold/delta-dim/dim_tag",
                "s3://gold/delta-dim/dim_tag_problem"]

# Read Delta table using deltalake (arrow-based)
for path in table_paths:
    dt = DeltaTable(path, storage_options={
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "AWS_ENDPOINT_URL": "http://localhost:9049",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_ADDRESSING_STYLE": "path"
    })

    # Convert to Polars
    df = pl.from_arrow(dt.to_pyarrow_table())
    print(df)