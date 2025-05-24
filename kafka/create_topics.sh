#!/bin/bash

BOOTSTRAP_SERVER="localhost:19092"
REQUIRED_TOPICS=("cleaned" "submissions")

# Lấy danh sách các topic hiện có
EXISTING_TOPICS=$(/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP_SERVER")

for topic in "${REQUIRED_TOPICS[@]}"; do
    if echo "$EXISTING_TOPICS" | grep -q "^${topic}$"; then
        echo "Topic '${topic}' đã tồn tại."
    else
        echo "Topic '${topic}' chưa tồn tại. Đang tạo..."
        /opt/kafka/bin/kafka-topics.sh --create \
            --topic "$topic" \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --partitions 1 \
            --replication-factor 1
    fi
done