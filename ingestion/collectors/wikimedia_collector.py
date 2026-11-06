
import logging
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
)

HEADERS = {
    "User-Agent": (
        "TrendCast-BigData-StudentProject/1.0 "
        "(contact: nou.soulaimane@gmail.com)"
    )
}

DEFAULT_PAGES = [
    "Python_(programming_language)",
    "Artificial_intelligence",
    "Big_data",
    "Machine_learning",
    "Cloud_computing",
]


def get_pages() -> list[str]:
    """Load the pages to collect from environment variables."""
    pages = os.getenv("WIKIMEDIA_PAGES")

    if not pages:
        return DEFAULT_PAGES

    return [
        page.strip()
        for page in pages.split(",")
        if page.strip()
    ]


def create_session() -> requests.Session:
    """Create an HTTP session with retry handling."""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    return session


def validate_date(date_value: str) -> None:
    """Validate Wikimedia API date format: YYYYMMDDHH."""
    try:
        datetime.strptime(date_value, "%Y%m%d%H")
    except ValueError as exc:
        raise ValueError(
            f"Invalid date format: {date_value}. "
            "Expected YYYYMMDDHH."
        ) from exc


def collect_pageviews(
    page_title: str,
    start: str,
    end: str,
    project: str = "en.wikipedia",
) -> list[dict]:
    """Collect daily Wikimedia pageview events."""
    validate_date(start)
    validate_date(end)

    if start > end:
        raise ValueError("Start date must be before end date.")

    url = (
        f"{BASE_URL}/{project}/all-access/user/"
        f"{page_title}/daily/{start}/{end}"
    )

    session = create_session()

    try:
        response = session.get(
            url,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()

        items = response.json().get("items", [])
        events = []

        for item in items:
            timestamp = datetime.strptime(
                item["timestamp"],
                "%Y%m%d%H",
            ).replace(tzinfo=timezone.utc)

            event = {
                "event_id": (
                    f"{project}:{page_title}:{item['timestamp']}"
                ),
                "source": "wikimedia_pageviews",
                "project": project,
                "page_title": page_title,
                "timestamp": timestamp.isoformat(),
                "granularity": "daily",
                "views": int(item["views"]),
                "schema_version": "1.0",
            }

            events.append(event)

        return events

    finally:
        session.close()


def collect_all(
    start: str | None = None,
    end: str | None = None,
    project: str = "en.wikipedia",
) -> list[dict]:
    """Collect pageviews for all configured pages."""
    start = start or os.getenv(
        "WIKIMEDIA_START",
        "2026100100",
    )
    end = end or os.getenv(
        "WIKIMEDIA_END",
        "2026100700",
    )

    all_events = []
    pages = get_pages()

    logger.info(
        "Starting Wikimedia collection for %s pages",
        len(pages),
    )

    for page in pages:
        try:
            events = collect_pageviews(
                page_title=page,
                start=start,
                end=end,
                project=project,
            )

            all_events.extend(events)

            logger.info(
                "Collected %s events for %s",
                len(events),
                page,
            )

        except (requests.RequestException, ValueError):
            logger.exception(
                "Collection failed for %s",
                page,
            )

    logger.info(
        "Total events collected: %s",
        len(all_events),
    )

    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result = collect_all()

    print(f"Total events collected: {len(result)}")

    for event in result[:5]:
        print(event)
