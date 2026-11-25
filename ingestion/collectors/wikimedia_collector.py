
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

HEADERS = {
    "User-Agent": (
        "TrendCast-BigData-StudentProject/1.0 "
        "(contact: nou.soulaimane@gmail.com)"
    )
}

MAX_DAYS_PER_REQUEST = 90

DEFAULT_PAGES = [
    "Python_(programming_language)",
    "Java_(programming_language)",
    "JavaScript",
    "Linux",
    "Git",
    "Cloud_computing",
    "Artificial_intelligence",
    "Machine_learning",
    "Deep_learning",
    "Big_data",
    "Data_science",
    "Neural_network",
    "Natural_language_processing",
    "ChatGPT",
    "OpenAI",
    "Microsoft",
    "Google",
    "NVIDIA",
    "Amazon_(company)",
    "Blockchain",
]


class WikimediaNoDataError(requests.HTTPError):
    """Wikimedia returned 404 for a page/date range without data."""


def get_pages() -> list[str]:
    """Return unique page titles from the environment or defaults."""
    pages = os.getenv("WIKIMEDIA_PAGES")

    if not pages:
        return DEFAULT_PAGES.copy()

    result = list(
        dict.fromkeys(
            page.strip()
            for page in pages.split(",")
            if page.strip()
        )
    )

    if not result:
        raise ValueError(
            "WIKIMEDIA_PAGES contains no valid page titles."
        )

    return result


def create_session() -> requests.Session:
    """Create an HTTP session with retry support."""
    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=4,
        pool_maxsize=4,
    )

    session.mount("https://", adapter)
    session.headers.update(HEADERS)

    return session


def validate_date(date_value: str) -> datetime:
    """Validate a Wikimedia daily date in YYYYMMDD00 format."""
    try:
        parsed = datetime.strptime(date_value, "%Y%m%d%H")
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Invalid date format: {date_value}. "
            "Expected YYYYMMDD00."
        ) from exc

    if parsed.hour != 0:
        raise ValueError(
            f"Daily collection requires hour 00: {date_value}."
        )

    return parsed.replace(tzinfo=timezone.utc)


def get_date_range(
    granularity: str,
    days_back: int,
) -> tuple[str, str]:
    """Build a date range ending yesterday (UTC)."""
    if granularity != "daily":
        raise ValueError(
            "Only daily Wikimedia collection is supported."
        )

    if days_back <= 0:
        raise ValueError(
            "WIKIMEDIA_DAYS_BACK must be positive."
        )

    yesterday = (
        datetime.now(timezone.utc).date()
        - timedelta(days=1)
    )

    start_date = yesterday - timedelta(days=days_back - 1)

    return (
        start_date.strftime("%Y%m%d00"),
        yesterday.strftime("%Y%m%d00"),
    )


def split_date_range(
    start: str,
    end: str,
    max_days: int = MAX_DAYS_PER_REQUEST,
) -> list[tuple[str, str]]:
    """Split a date range into API-compatible windows."""
    start_date = validate_date(start)
    end_date = validate_date(end)

    if start_date > end_date:
        raise ValueError(
            "Start date must be before or equal to end date."
        )

    if max_days <= 0:
        raise ValueError("max_days must be positive.")

    ranges = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=max_days - 1),
            end_date,
        )

        ranges.append(
            (
                current_start.strftime("%Y%m%d00"),
                current_end.strftime("%Y%m%d00"),
            )
        )

        current_start = current_end + timedelta(days=1)

    return ranges


def collect_pageviews(
    page_title: str,
    start: str,
    end: str,
    project: str = "en.wikipedia",
    granularity: str = "daily",
    session: requests.Session | None = None,
) -> list[dict]:
    """Collect daily pageviews for one Wikimedia page."""
    validate_date(start)
    validate_date(end)

    if start > end:
        raise ValueError(
            "Start date must be before or equal to end date."
        )

    if granularity != "daily":
        raise ValueError(
            "Only daily Wikimedia collection is supported."
        )

    url = (
        f"{BASE_URL}/{quote(project, safe='')}/all-access/user/"
        f"{quote(page_title, safe='')}/daily/{start}/{end}"
    )

    own_session = session is None
    client = session or create_session()

    try:
        response = client.get(url, timeout=(10, 60))

        if response.status_code == 404:
            raise WikimediaNoDataError(
                f"Wikimedia returned 404: page={page_title}, "
                f"start={start}, end={end}",
                response=response,
            )

        response.raise_for_status()

        items = response.json().get("items", [])
        events = []

        for item in items:
            raw_timestamp = item.get("timestamp")
            raw_views = item.get("views")

            if not raw_timestamp or raw_views is None:
                logger.warning(
                    "Skipping incomplete item for page=%s",
                    page_title,
                )
                continue

            timestamp = datetime.strptime(
                raw_timestamp,
                "%Y%m%d%H",
            ).replace(tzinfo=timezone.utc)

            events.append(
                {
                    "event_id": (
                        f"{project}:{page_title}:{raw_timestamp}"
                    ),
                    "source": "wikimedia_pageviews",
                    "timestamp": timestamp.isoformat(),
                    "schema_version": "1.0",
                    "payload": {
                        "project": project,
                        "page_title": page_title,
                        "granularity": "daily",
                        "views": int(raw_views),
                    },
                }
            )

        return events

    finally:
        if own_session:
            client.close()


def filter_new_events(
    events: list[dict],
    existing_event_ids: set[str] | None = None,
) -> list[dict]:
    """
    Keep only IDs not already stored and remove duplicates
    within the current collection.
    """
    known_ids = set(existing_event_ids or set())
    new_events = []

    for event in events:
        event_id = event["event_id"]

        if event_id in known_ids:
            continue

        known_ids.add(event_id)
        new_events.append(event)

    skipped = len(events) - len(new_events)

    logger.info(
        "Incremental filtering: received=%s new=%s skipped=%s",
        len(events),
        len(new_events),
        skipped,
    )

    return new_events


def collect_all(
    start: str | None = None,
    end: str | None = None,
    project: str | None = None,
    granularity: str | None = None,
    days_back: int | None = None,
    existing_event_ids: set[str] | None = None,
) -> list[dict]:
    """
    Collect Wikimedia events and exclude IDs already published.

    existing_event_ids must contain IDs loaded from the persistent
    ingestion registry, including IDs imported from historical HDFS.
    """
    granularity = (
        granularity
        or os.getenv("WIKIMEDIA_GRANULARITY", "daily")
    )

    if granularity != "daily":
        raise ValueError(
            "WIKIMEDIA_GRANULARITY must be daily."
        )

    project = project or os.getenv(
        "WIKIMEDIA_PROJECT",
        "en.wikipedia",
    )

    configured_start = start or os.getenv("WIKIMEDIA_START")
    configured_end = end or os.getenv("WIKIMEDIA_END")

    if configured_start and configured_end:
        start_date = configured_start
        end_date = configured_end

    elif configured_start or configured_end:
        raise ValueError(
            "WIKIMEDIA_START and WIKIMEDIA_END "
            "must be provided together."
        )

    else:
        configured_days = days_back or int(
            os.getenv("WIKIMEDIA_DAYS_BACK", "90")
        )

        start_date, end_date = get_date_range(
            granularity,
            configured_days,
        )

    validate_date(start_date)
    validate_date(end_date)

    if start_date > end_date:
        raise ValueError(
            "Start date must be before or equal to end date."
        )

    pages = get_pages()
    date_ranges = split_date_range(
        start_date,
        end_date,
    )

    request_delay = float(
        os.getenv("WIKIMEDIA_REQUEST_DELAY_SECONDS", "0.2")
    )

    if request_delay < 0:
        raise ValueError(
            "WIKIMEDIA_REQUEST_DELAY_SECONDS "
            "cannot be negative."
        )

    planned_requests = len(pages) * len(date_ranges)

    all_events = []
    successful_requests = 0
    failed_requests = 0

    logger.info(
        "Wikimedia collection: pages=%s start=%s end=%s "
        "windows=%s planned_requests=%s delay=%ss",
        len(pages),
        start_date,
        end_date,
        len(date_ranges),
        planned_requests,
        request_delay,
    )

    with create_session() as session:
        request_number = 0

        for page_index, page in enumerate(pages, start=1):
            page_count = 0

            logger.info(
                "Processing page %s/%s: %s",
                page_index,
                len(pages),
                page,
            )

            for window_start, window_end in date_ranges:
                request_number += 1

                try:
                    events = collect_pageviews(
                        page,
                        window_start,
                        window_end,
                        project=project,
                        granularity=granularity,
                        session=session,
                    )

                    all_events.extend(events)
                    page_count += len(events)
                    successful_requests += 1

                    logger.info(
                        "Request %s/%s succeeded: page=%s "
                        "window=%s..%s events=%s",
                        request_number,
                        planned_requests,
                        page,
                        window_start,
                        window_end,
                        len(events),
                    )

                except WikimediaNoDataError:
                    successful_requests += 1

                    logger.warning(
                        "No data: page=%s window=%s..%s",
                        page,
                        window_start,
                        window_end,
                    )

                except (
                    requests.RequestException,
                    ValueError,
                    KeyError,
                ):
                    failed_requests += 1

                    logger.exception(
                        "Failed to collect Wikimedia data for window %s - %s",
                        window_start,
                        window_end,
                    )

                if request_delay:
                    time.sleep(request_delay)

            logger.info(
                "Collected %s events for page=%s",
                page_count,
                page,
            )

    new_events = filter_new_events(
        all_events,
        existing_event_ids=existing_event_ids,
    )

    logger.info(
        "Wikimedia completed: pages=%s collected=%s new=%s "
        "successful=%s failed=%s",
        len(pages),
        len(all_events),
        len(new_events),
        successful_requests,
        failed_requests,
    )

    if failed_requests:
        raise RuntimeError(
            f"Wikimedia collection incomplete: "
            f"{failed_requests}/{planned_requests} requests failed. "
            "No events will be published. "
            "Correct the errors and retry the collection."
        )

    return new_events


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(name)s - %(message)s"
        ),
    )

    result = collect_all()

    print(f"Total new events collected: {len(result)}")

    for event in result[:5]:
        print(event)
