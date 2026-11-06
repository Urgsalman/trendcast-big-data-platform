
#!/bin/bash

set -e

KAFKA_CONTAINER="trendcast-kafka"

echo "Creating Kafka topics..."

docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic pageviews.validated \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Kafka topics created successfully."
