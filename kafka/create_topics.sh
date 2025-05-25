#!/bin/bash


TOPIC_NAMES=("submissions" "flatten", "cleaned")

for TOPIC_NAME in "${TOPIC_NAMES[@]}"; do
  EXISTING_TOPIC=$(/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:19092 | grep "^$TOPIC_NAME$")

  if [ -z "$EXISTING_TOPIC" ]; then
    echo "[$TOPIC_NAME] does not exist. Creating..."
    /opt/kafka/bin/kafka-topics.sh --create --topic "$TOPIC_NAME" --bootstrap-server localhost:19092 --partitions 1 --replication-factor 1
  else
    echo "[$TOPIC_NAME] already exists. Skipping creation."
  fi
done
