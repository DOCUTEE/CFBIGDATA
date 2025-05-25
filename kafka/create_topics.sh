#!/bin/bash

REQUIRED_TOPICS=(
    "submissions"
    "cleaned"
)

for topic in "${REQUIRED_TOPICS[@]}"; do
    # Kiểm tra nếu topic đã tồn tại
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list | grep -q "^${topic}$"; then
        echo "Topic '${topic}' đã tồn tại. Đang xóa..."
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic $topic

    fi

    echo "Đang tạo lại topic '${topic}'..."
    /opt/kafka/bin/kafka-topics.sh --create \
        --topic "$topic" \
        --bootstrap-server "localhost:19092"
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic submissions