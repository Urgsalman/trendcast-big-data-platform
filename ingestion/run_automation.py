"""Periodic ingestion with persistent receipts and per-page historical catch-up."""

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from ingestion.automation_state import PublicationState
from ingestion.collectors.google_trends_collector import collect_trends
from ingestion.collectors.hackernews_collector import collect_stories
from ingestion.collectors.wikimedia_collector import (
    collect_pageviews,
    create_session,
    get_pages,
)
from ingestion.producers.kafka_producer import KafkaEventProducer
from ingestion.validators.event_validator import validate_event

logger = logging.getLogger(__name__)
SOURCES = ("wikimedia_pageviews", "hackernews", "google_trends")
DEFAULT_INTERVALS = {
    "wikimedia_pageviews": 86400,
    "hackernews": 1800,
    "google_trends": 3600,
}
INTERVAL_VARIABLES = {
    "wikimedia_pageviews": "WIKIMEDIA_INTERVAL_SECONDS",
    "hackernews": "HACKERNEWS_INTERVAL_SECONDS",
    "google_trends": "GOOGLE_TRENDS_INTERVAL_SECONDS",
}


def positive_setting(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def interval_seconds(source: str) -> int:
    return positive_setting(INTERVAL_VARIABLES[source], DEFAULT_INTERVALS[source])


def publication_key(namespace: str, event: dict, slot: str = "") -> str:
    return json.dumps([namespace, event["source"], event["event_id"], slot])


def missing_ranges(start: date, end: date, confirmed) -> list[tuple[date, date]]:
    """Group consecutive missing dates without skipping isolated holes."""
    ranges = []
    first = None
    current = start
    while current <= end:
        if not confirmed(current):
            if first is None:
                first = current
        elif first is not None:
            ranges.append((first, current - timedelta(days=1)))
            first = None
        current += timedelta(days=1)
    if first is not None:
        ranges.append((first, end))
    return ranges


class AutomationRunner:
    def __init__(self, state: PublicationState, producer_factory=KafkaEventProducer):
        self.state = state
        self.producer_factory = producer_factory
        self.producer = None
        # A receipt for one destination must never suppress another destination.
        self.namespace = json.dumps(
            [
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                os.getenv("KAFKA_TOPIC", "trendcast.events.validated"),
            ]
        )
        self.failed = False
        self.published = 0
        self.skipped = 0

    def publish(self, event: dict, slot: str = "") -> None:
        try:
            validated = validate_event(event)
            key = publication_key(self.namespace, validated, slot)
            if self.state.contains(key):
                self.skipped += 1
                return
            if self.producer is None:
                self.producer = self.producer_factory()
            self.producer.publish(validated)
            self.state.confirm(key)
            self.published += 1
        except Exception:
            self.failed = True
            logger.exception(
                "Publication not confirmed: event=%s", event.get("event_id")
            )

    def collect_wikimedia(self, now: datetime) -> None:
        if os.getenv("WIKIMEDIA_GRANULARITY", "daily") != "daily":
            raise ValueError(
                "Automated Wikimedia collection requires daily granularity"
            )
        if os.getenv("WIKIMEDIA_START") or os.getenv("WIKIMEDIA_END"):
            raise ValueError("Remove WIKIMEDIA_START/END for automated date management")
        days = positive_setting("WIKIMEDIA_DAYS_BACK", 90)
        end = now.date() - timedelta(days=1)
        initial = end - timedelta(days=days - 1)
        project = os.getenv("WIKIMEDIA_PROJECT", "en.wikipedia")
        with create_session() as session:
            for page in get_pages():
                series = json.dumps([self.namespace, project, page, "daily"])
                start = date.fromisoformat(
                    self.state.history_start(series, initial.isoformat())
                )

                def key_for(day, page=page):
                    return publication_key(
                        self.namespace,
                        {
                            "source": "wikimedia_pageviews",
                            "event_id": f"{project}:{page}:{day:%Y%m%d}00",
                        },
                    )

                ranges = missing_ranges(
                    start, end, lambda day: self.state.contains(key_for(day))
                )
                for lower, upper in ranges:
                    try:
                        events = collect_pageviews(
                            page,
                            f"{lower:%Y%m%d}00",
                            f"{upper:%Y%m%d}00",
                            project=project,
                            granularity="daily",
                            session=session,
                        )
                        for event in events:
                            self.publish(event)
                        # An empty/partial API result does not advance a watermark.
                        day = lower
                        while day <= upper:
                            if not self.state.contains(key_for(day)):
                                self.failed = True
                                logger.warning(
                                    "Wikimedia pending: page=%s date=%s", page, day
                                )
                            day += timedelta(days=1)
                    except Exception:
                        self.failed = True
                        logger.exception("Wikimedia range failed: page=%s", page)

    def run_source(self, source: str, now: datetime) -> bool:
        self.failed = False
        self.published = 0
        self.skipped = 0
        try:
            if source == "wikimedia_pageviews":
                self.collect_wikimedia(now)
            else:
                # One observation per UTC time bucket; retain the original envelope.
                seconds = interval_seconds(source)
                slot = f"{seconds}:{int(now.timestamp()) // seconds}"
                collector = {
                    "hackernews": collect_stories,
                    "google_trends": collect_trends,
                }[source]
                events = collector()
                if not events:
                    logger.warning("No events returned: source=%s", source)
                for event in events:
                    self.publish(event, slot)
        except Exception:
            self.failed = True
            logger.exception("Collection failed: source=%s", source)
        finally:
            if self.producer is not None:
                try:
                    self.producer.close()
                except Exception:
                    self.failed = True
                    logger.exception("Producer close failed")
                self.producer = None
        logger.info(
            "Source=%s published=%s skipped=%s failed=%s",
            source,
            self.published,
            self.skipped,
            self.failed,
        )
        return not self.failed


def execute_source(source: str, state_path: str) -> bool:
    state = PublicationState(state_path)
    try:
        return AutomationRunner(state).run_source(source, datetime.now(timezone.utc))
    finally:
        state.close()


def build_scheduler(state_path: str) -> BlockingScheduler:
    # A single worker serializes collection jobs; one scheduler process per state DB.
    scheduler = BlockingScheduler(
        timezone=timezone.utc,
        executors={"default": ThreadPoolExecutor(max_workers=1)},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": None},
    )
    now = datetime.now(timezone.utc)
    for source in SOURCES:
        scheduler.add_job(
            execute_source,
            "interval",
            seconds=interval_seconds(source),
            args=[source, state_path],
            id=source,
            replace_existing=True,
            next_run_time=now,
        )
    return scheduler


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run selected sources once")
    parser.add_argument("--source", choices=("all", *SOURCES), default="all")
    args = parser.parse_args()
    if not args.once and args.source != "all":
        parser.error("--source requires --once")
    state_path = os.getenv("INGESTION_STATE_PATH", ".trendcast/ingestion.sqlite3")
    if args.once:
        sources = SOURCES if args.source == "all" else (args.source,)
        results = [execute_source(source, state_path) for source in sources]
        return 0 if all(results) else 1
    scheduler = build_scheduler(state_path)
    logger.info("Starting periodic ingestion; state=%s", state_path)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        if scheduler.running:
            scheduler.shutdown(wait=True)
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    raise SystemExit(main())
