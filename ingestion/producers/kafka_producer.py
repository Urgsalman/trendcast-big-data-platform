
import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.topic = topic or os.getenv(
            "KAFKA_TOPIC",
            "trendcast.events.validated",
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=1000,
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,
            linger_ms=10,
            compression_type="gzip",
        )

        logger.info(
            "Kafka producer initialized: server=%s topic=%s",
            self.bootstrap_servers,
            self.topic,
        )

    def publish(self, event: dict) -> None:
        """Publish one validated event and wait for confirmation."""
        event_id = event["event_id"]

        future = self.producer.send(
            self.topic,
            key=event_id,
            value=event,
        )

        metadata = future.get(timeout=60)

        logger.info(
            "Published event=%s topic=%s partition=%s offset=%s",
            event_id,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    def close(self) -> None:
        """Flush pending messages and close the connection."""
        if self.producer is not None:
            self.producer.flush(timeout=30)
            self.producer.close(timeout=30)
            logger.info("Kafka producer connection closed")
