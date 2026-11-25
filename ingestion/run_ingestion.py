
import argparse
import logging
import os
import sqlite3
from pathlib import Path

import requests
from dotenv import load_dotenv

from ingestion.collectors.google_trends_collector import collect_trends
from ingestion.collectors.hackernews_collector import collect_stories
from ingestion.collectors.wikimedia_collector import collect_all
from ingestion.producers.kafka_producer import KafkaEventProducer
from ingestion.validators.event_validator import validate_event

load_dotenv()

logger = logging.getLogger(__name__)

COLLECTORS = {
    "wikimedia": collect_all,
    "hackernews": collect_stories,
    "google_trends": collect_trends,
}

DEFAULT_STATE_PATH = ".trendcast/ingestion.sqlite3"

WIKIMEDIA_SOURCE = "wikimedia_pageviews"

PERSISTENT_SOURCES = {
    "hackernews",
    "google_trends",
}


def get_state_path() -> Path:
    """Return the SQLite state database path."""
    return Path(
        os.getenv(
            "INGESTION_STATE_PATH",
            DEFAULT_STATE_PATH,
        )
    )


def initialize_state() -> None:
    """Create deduplication tables without deleting existing state."""
    state_path = get_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(state_path, timeout=30) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS wikimedia_published_events (
                event_id TEXT PRIMARY KEY,
                published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS source_published_events (
                source TEXT NOT NULL,
                event_id TEXT NOT NULL,
                published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source, event_id)
            )
            """
        )

        connection.commit()


def initialize_wikimedia_state() -> None:
    """Backward-compatible Wikimedia state initialization."""
    initialize_state()


def load_wikimedia_event_ids() -> set[str]:
    """
    Load known Wikimedia event IDs.

    The minimum count protects existing HDFS history from being
    republished when the local registry has not been seeded.
    """
    initialize_state()

    minimum_ids = int(
        os.getenv("WIKIMEDIA_DEDUP_MIN_IDS", "30971")
    )

    if minimum_ids < 0:
        raise ValueError(
            "WIKIMEDIA_DEDUP_MIN_IDS cannot be negative."
        )

    state_path = get_state_path()

    with sqlite3.connect(state_path, timeout=30) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM wikimedia_published_events
            """
        ).fetchone()

        stored_count = row[0] if row else 0

        if stored_count < minimum_ids:
            raise RuntimeError(
                "Wikimedia deduplication registry is not initialized: "
                f"{stored_count} IDs found, at least {minimum_ids} "
                "required. Import the existing HDFS event IDs into "
                "the SQLite table before running Wikimedia ingestion."
            )

        rows = connection.execute(
            """
            SELECT event_id
            FROM wikimedia_published_events
            """
        ).fetchall()

    event_ids = {row[0] for row in rows}

    logger.info(
        "Wikimedia deduplication registry loaded: %s IDs",
        len(event_ids),
    )

    return event_ids


def save_wikimedia_event_ids(event_ids: set[str]) -> None:
    """Persist successfully published Wikimedia event IDs."""
    if not event_ids:
        return

    initialize_state()

    state_path = get_state_path()

    with sqlite3.connect(state_path, timeout=30) as connection:
        connection.executemany(
            """
            INSERT OR IGNORE INTO wikimedia_published_events (event_id)
            VALUES (?)
            """,
            [(event_id,) for event_id in event_ids],
        )

        connection.commit()

    logger.info(
        "Wikimedia registry updated with %s published IDs",
        len(event_ids),
    )


def load_source_event_ids(source: str) -> set[str]:
    """Load previously published event IDs for a source."""
    if source not in PERSISTENT_SOURCES:
        raise ValueError(f"Unsupported persistent source: {source}")

    initialize_state()
    state_path = get_state_path()

    with sqlite3.connect(state_path, timeout=30) as connection:
        rows = connection.execute(
            """
            SELECT event_id
            FROM source_published_events
            WHERE source = ?
            """,
            (source,),
        ).fetchall()

    event_ids = {row[0] for row in rows}

    logger.info(
        "%s deduplication registry loaded: %s IDs",
        source,
        len(event_ids),
    )

    return event_ids


def save_source_event_ids(
    source: str,
    event_ids: set[str],
) -> None:
    """Persist successfully published IDs for a non-Wikimedia source."""
    if not event_ids:
        return

    if source not in PERSISTENT_SOURCES:
        raise ValueError(f"Unsupported persistent source: {source}")

    initialize_state()
    state_path = get_state_path()

    with sqlite3.connect(state_path, timeout=30) as connection:
        connection.executemany(
            """
            INSERT OR IGNORE INTO source_published_events
                (source, event_id)
            VALUES (?, ?)
            """,
            [
                (source, event_id)
                for event_id in event_ids
            ],
        )

        connection.commit()

    logger.info(
        "%s registry updated with %s published IDs",
        source,
        len(event_ids),
    )


def parse_args() -> argparse.Namespace:
    """Parse ingestion command-line arguments."""
    parser = argparse.ArgumentParser(
        description="TrendCast multi-source Kafka ingestion."
    )

    parser.add_argument(
        "--source",
        choices=["all", *COLLECTORS.keys()],
        default="all",
        help="Source to collect. Defaults to all sources.",
    )

    parser.add_argument(
        "--start",
        help="Wikimedia start date in YYYYMMDD00 format.",
    )

    parser.add_argument(
        "--end",
        help="Wikimedia end date in YYYYMMDD00 format.",
    )

    parser.add_argument(
        "--days-back",
        type=int,
        help=(
            "Number of historical days for Wikimedia. "
            "Overrides WIKIMEDIA_DAYS_BACK."
        ),
    )

    args = parser.parse_args()

    if bool(args.start) != bool(args.end):
        parser.error(
            "--start and --end must be provided together."
        )

    if args.source != "wikimedia" and (
        args.start or args.end or args.days_back is not None
    ):
        parser.error(
            "--start, --end and --days-back are only "
            "supported with --source wikimedia."
        )

    if args.days_back is not None and args.days_back <= 0:
        parser.error("--days-back must be positive.")

    return args


def collect_selected_sources(
    source: str,
    start: str | None = None,
    end: str | None = None,
    days_back: int | None = None,
) -> list[dict]:
    """Collect events from the selected source or all sources."""
    if source == "all":
        selected_collectors = COLLECTORS
    else:
        selected_collectors = {
            source: COLLECTORS[source],
        }

    all_events = []

    for source_name, collector in selected_collectors.items():
        try:
            if source_name == "wikimedia":
                existing_ids = load_wikimedia_event_ids()

                events = collector(
                    start=start,
                    end=end,
                    days_back=days_back,
                    existing_event_ids=existing_ids,
                )

            elif source_name in PERSISTENT_SOURCES:
                existing_ids = load_source_event_ids(source_name)

                events = collector(
                    existing_event_ids=existing_ids,
                )

            else:
                events = collector()

            all_events.extend(events)

            logger.info(
                "Source=%s collected=%s",
                source_name,
                len(events),
            )

        except (
            requests.RequestException,
            ValueError,
            TypeError,
            RuntimeError,
            sqlite3.Error,
        ):
            logger.exception(
                "Collection failed for source=%s",
                source_name,
            )

    return all_events


def publish_events(
    events: list[dict],
) -> tuple[int, int, dict[str, set[str]]]:
    """
    Validate and publish events to Kafka.

    Returns:
      - published count
      - rejected count
      - successfully published event IDs grouped by source
    """
    if not events:
        logger.warning(
            "No events collected. Kafka producer not started."
        )
        return 0, 0, {}

    producer = KafkaEventProducer()

    published = 0
    rejected = 0
    published_ids_by_source: dict[str, set[str]] = {}

    try:
        for event in events:
            try:
                validated_event = validate_event(event)
                producer.publish(validated_event)
                published += 1

                event_source = event.get("source")
                event_id = event.get("event_id")

                if event_source and event_id:
                    published_ids_by_source.setdefault(
                        event_source,
                        set(),
                    ).add(event_id)

            except Exception:
                logger.exception(
                    "Event rejected or publication failed: %s",
                    event.get("event_id"),
                )
                rejected += 1

    finally:
        producer.close()

    return (
        published,
        rejected,
        published_ids_by_source,
    )


def persist_published_ids(
    published_ids_by_source: dict[str, set[str]],
) -> None:
    """Save confirmed Kafka publications in the appropriate registry."""
    for source_name, event_ids in published_ids_by_source.items():
        try:
            if source_name == WIKIMEDIA_SOURCE:
                save_wikimedia_event_ids(event_ids)

            elif source_name in PERSISTENT_SOURCES:
                save_source_event_ids(source_name, event_ids)

        except sqlite3.Error:
            logger.exception(
                "Kafka publication succeeded, but SQLite registry "
                "could not be updated for source=%s. Check Kafka "
                "and the registry before retrying.",
                source_name,
            )


def run() -> None:
    """Collect, validate and publish events to Kafka."""
    args = parse_args()

    logger.info(
        "Starting ingestion: source=%s",
        args.source,
    )

    events = collect_selected_sources(
        source=args.source,
        start=args.start,
        end=args.end,
        days_back=args.days_back,
    )

    logger.info(
        "Total collected events: %s",
        len(events),
    )

    published, rejected, published_ids_by_source = publish_events(
        events
    )

    persist_published_ids(published_ids_by_source)

    logger.info("Total collected: %s", len(events))
    logger.info("Published events: %s", published)
    logger.info("Rejected events: %s", rejected)

    for source_name, event_ids in published_ids_by_source.items():
        logger.info(
            "Saved IDs: source=%s count=%s",
            source_name,
            len(event_ids),
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(name)s - %(message)s"
        ),
    )

    run()
