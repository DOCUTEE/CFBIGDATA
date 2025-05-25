from kafka import KafkaProducer
import pandas as pd
import json
import time

# Đường dẫn file parquet
PARQUET_FILE_PATH = '/opt/fake-stream/Submission_contest_2096.parquet'  

# Tên topic Kafka
KAFKA_TOPIC = 'submissions'

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers='cf-broker:19092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# limit = -1
limit = 10

# Đọc dữ liệu từ file Parquet
df = pd.read_parquet(PARQUET_FILE_PATH)

if (limit > 0):
    df = df.head(limit)

print(df)

# Ghi nhận thời gian bắt đầu chạy chương trình
start_time = time.time()

for i, row in df.iterrows():
    # Tính thời gian cần đợi trước khi gửi dòng này
    target_time = row["relativeTimeSeconds"]
    current_elapsed = time.time() - start_time
    wait_time = target_time - current_elapsed

    # Đợi đến thời điểm thích hợp nếu cần
    if wait_time > 0:
        time.sleep(wait_time)

    # Chuyển dòng thành dict để gửi Kafka
    message = row.to_dict()

    # Gửi lên Kafka
    producer.send(KAFKA_TOPIC, value=message)
    print(f"[{round(time.time() - start_time)}s] Sent: {message}")

producer.send("submissions", value={"id": -1})
print(f"[{round(time.time() - start_time)}s] Sent: end signal")


producer.flush()
producer.close()
