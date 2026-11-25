
#!/bin/bash

set -e

KAFKA_CONTAINER="${KAFKA_CONTAINER:-trendcast-kafka}"
KAFKA_TOPIC="${KAFKA_TOPIC:-trendcast.events.validated}"

echo "Creating Kafka topics..."

docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Kafka topics created successfully."
