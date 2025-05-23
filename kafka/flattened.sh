/opt/kafka/bin/kafka-topics.sh --create --topic sentences --bootstrap-server localhost:19092
/opt/kafka/bin/kafka-console-producer.sh --topic sentences --bootstrap-server localhost:19092
/opt/kafka/bin/kafka-console-consumer.sh --topic sentences --bootstrap-server localhost:9092 --from-beginning

/opt/kafka/bin/kafka-topics.sh --delete --topic flatten-to-clickhouse --bootstrap-server localhost:9092