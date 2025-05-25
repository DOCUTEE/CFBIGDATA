import json
import random
import time
from kafka import KafkaProducer

# Kafka config
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'cleaned'

# Cấu hình submission
handles = ["FortuneWillWinICPC", "DOCUTEE", "binhball192004"]
languages = ["Python 3", "GNU C++20 (64)", "GNU C++17", "Java 17", "Rust 2021", "Go"]
verdicts = ["OK", "WRONG_ANSWER", "COMPILATION_ERROR", "TIME_LIMIT_EXCEEDED", "RUNTIME_ERROR"]
base_id = 316234700

# Problem cố định
problem = {
    "contestId": 2096,
    "index": "A",
    "name": "Wonderful Sticks",
    "points": 500.0,
    "rating": 800,
    "tags": ["constructive algorithms", "greedy"]
}

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sinh và gửi submission ngẫu nhiên
for i in range(10):  # Gửi 10 bản ghi
    verdict = random.choice(verdicts)
    submission = {
        "id": base_id + i,
        "relativeTimeSeconds": random.randint(30, 200),
        "programmingLanguage": random.choice(languages),
        "verdict": verdict,
        "passedTestCount": random.randint(0, 15) if verdict != "COMPILATION_ERROR" else 0,
        "timeConsumedMillis": random.randint(0, 2000) if verdict != "COMPILATION_ERROR" else 0,
        "memoryConsumedBytes": random.choice([0, 122880, 172032, 196608, 229376, 393216, 458752]) if verdict != "COMPILATION_ERROR" else 0,
        "problem": problem,
        "handle": random.choice(handles)
    }

    producer.send(TOPIC, value=submission)
    print(f"Sent: {submission['id']}")
    time.sleep(1)

producer.flush()
producer.close()
