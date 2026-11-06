
import json
import logging

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


def consume_events():
    consumer = KafkaConsumer(
        "pageviews.validated",
        bootstrap_servers="localhost:9092",
        group_id="trendcast-validation-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    logger.info("Kafka consumer started")

    try:
        for message in consumer:
            event = message.value

            print(
                f"Page={event['page_title']} "
                f"Views={event['views']} "
                f"Timestamp={event['timestamp']}"
            )

            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer stopped")

    finally:
        consumer.close()
        logger.info("Kafka consumer connection closed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consume_events()
