
import json
import logging

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class PageviewProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "pageviews.validated",
    ):
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=5,
        )

    def publish(self, event: dict) -> None:
        event_id = event["event_id"]

        future = self.producer.send(
            self.topic,
            key=event_id,
            value=event,
        )

        metadata = future.get(timeout=30)

        logger.info(
            "Published event=%s topic=%s partition=%s offset=%s",
            event_id,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer connection closed")
