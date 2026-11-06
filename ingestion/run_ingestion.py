
import logging

from ingestion.collectors.wikimedia_collector import collect_all
from ingestion.producers.kafka_producer import PageviewProducer
from ingestion.validators.event_validator import validate_event

logger = logging.getLogger(__name__)


def run():
    events = collect_all()

    producer = PageviewProducer()

    published = 0
    rejected = 0

    try:
        for event in events:
            try:
                validated_event = validate_event(event)
                producer.publish(validated_event)
                published += 1

            except Exception:
                logger.exception(
                    "Event rejected: %s",
                    event.get("event_id"),
                )
                rejected += 1

    finally:
        producer.close()

    logger.info("Published events: %s", published)
    logger.info("Rejected events: %s", rejected)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
