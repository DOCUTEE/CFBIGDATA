/opt/kafka/bin/kafka-topics.sh --delete --topic submissions --bootstrap-server localhost:19092
/opt/kafka/bin/kafka-console-consumer.sh --topic cleaned --bootstrap-server localhost:19092