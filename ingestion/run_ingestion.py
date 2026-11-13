
import logging

import requests

from ingestion.collectors.google_trends_collector import collect_trends
from ingestion.collectors.hackernews_collector import collect_stories
from ingestion.collectors.wikimedia_collector import collect_all
from ingestion.producers.kafka_producer import KafkaEventProducer
from ingestion.validators.event_validator import validate_event

logger = logging.getLogger(__name__)


def collect_all_sources() -> list[dict]:
    """Collect events from all configured sources."""
    collectors = {
        "wikimedia_pageviews": collect_all,
        "hackernews": collect_stories,
        "google_trends": collect_trends,
    }

    all_events = []

    for source, collector in collectors.items():
        try:
            events = collector()
            all_events.extend(events)

            logger.info(
                "Source=%s collected=%s",
                source,
                len(events),
            )

        except (requests.RequestException, ValueError, TypeError):
            logger.exception(
                "Collection failed for source=%s",
                source,
            )

    return all_events


def run() -> None:
    """Collect, validate and publish events to Kafka."""
    events = collect_all_sources()

    logger.info("Total collected events: %s", len(events))

    if not events:
        logger.warning("No events collected. Kafka producer not started.")
        return

    producer = KafkaEventProducer()

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
                    "Event rejected or publication failed: %s",
                    event.get("event_id"),
                )
                rejected += 1

    finally:
        producer.close()

    logger.info("Total collected: %s", len(events))
    logger.info("Published events: %s", published)
    logger.info("Rejected events: %s", rejected)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    run()
