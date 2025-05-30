import boto3
from botocore.exceptions import ClientError

# Thông tin kết nối MinIO
minio_endpoint = "http://cf-minio:9000"  # hoặc "http://minio:9000" nếu chạy trong container
access_key = "minio"
secret_key = "minio123"

# Khởi tạo client S3 tương thích MinIO
s3 = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1"  # MinIO không cần region thực
)

# Danh sách bucket cần tạo
buckets = ["silver", "gold"]

for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"✅ Created bucket: {bucket}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"⚠️ Bucket already exists: {bucket}")
        else:
            raise
