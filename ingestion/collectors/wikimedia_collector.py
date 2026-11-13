
import logging
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

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
    """Load configured Wikimedia pages."""
    pages = os.getenv("WIKIMEDIA_PAGES")

    if not pages:
        return DEFAULT_PAGES.copy()

    result = list(dict.fromkeys(
        page.strip()
        for page in pages.split(",")
        if page.strip()
    ))

    if not result:
        raise ValueError("WIKIMEDIA_PAGES contains no valid pages.")

    return result


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
    """Validate Wikimedia date format: YYYYMMDDHH."""
    try:
        datetime.strptime(date_value, "%Y%m%d%H")
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Invalid date format: {date_value}. "
            "Expected YYYYMMDDHH."
        ) from exc



def get_date_range(
    granularity: str,
    days_back: int,
) -> tuple[str, str]:
    """Generate a historical date range."""
    if days_back <= 0:
        raise ValueError("WIKIMEDIA_DAYS_BACK must be positive.")

    now = datetime.now(timezone.utc)

    if granularity == "daily":
        # Exclude today because its daily data may be incomplete.
        end_date = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ) - timedelta(days=1)

        start_date = end_date - timedelta(days=days_back - 1)

    elif granularity == "monthly":
        # Monthly support can be added separately if needed.
        raise ValueError(
            "Monthly collection is not configured yet."
        )

    else:
        raise ValueError(
            "Granularity must be 'daily'."
        )

    return (
        start_date.strftime("%Y%m%d%H"),
        end_date.strftime("%Y%m%d%H"),
    )



def collect_pageviews(
    page_title: str,
    start: str,
    end: str,
    project: str = "en.wikipedia",
    granularity: str = "hourly",
    session: requests.Session | None = None,
) -> list[dict]:
    """Collect Wikimedia pageview events for one page."""
    validate_date(start)
    validate_date(end)

    if start > end:
        raise ValueError("Start date must be before end date.")

    if granularity not in {"hourly", "daily"}:
        raise ValueError(
            "Granularity must be 'hourly' or 'daily'."
        )

    url = (
        f"{BASE_URL}/{quote(project, safe='')}/all-access/user/"
        f"{quote(page_title, safe='')}/{granularity}/{start}/{end}"
    )

    own_session = session is None
    client = session or create_session()

    try:
        response = client.get(
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
                "timestamp": timestamp.isoformat(),
                "schema_version": "1.0",
                "payload": {
                    "project": project,
                    "page_title": page_title,
                    "granularity": granularity,
                    "views": int(item["views"]),
                },
            }

            events.append(event)

        return events

    finally:
        if own_session:
            client.close()


def collect_all(
    start: str | None = None,
    end: str | None = None,
    project: str | None = None,
    granularity: str | None = None,
) -> list[dict]:
    """Collect pageviews for all configured pages."""
    granularity = (
        granularity
        or os.getenv("WIKIMEDIA_GRANULARITY", "hourly")
    )

    if granularity not in {"hourly", "daily"}:
        raise ValueError(
            "WIKIMEDIA_GRANULARITY must be hourly or daily."
        )

    project = project or os.getenv(
        "WIKIMEDIA_PROJECT",
        "en.wikipedia",
    )

    configured_start = start or os.getenv("WIKIMEDIA_START")
    configured_end = end or os.getenv("WIKIMEDIA_END")

    if configured_start and configured_end:
        start = configured_start
        end = configured_end
    elif configured_start or configured_end:
        raise ValueError(
            "WIKIMEDIA_START and WIKIMEDIA_END must be "
            "provided together."
        )
    else:
        days_back = int(
            os.getenv("WIKIMEDIA_DAYS_BACK", "30")
        )
        start, end = get_date_range(granularity, days_back)

    validate_date(start)
    validate_date(end)

    if start > end:
        raise ValueError("Start date must be before end date.")

    pages = get_pages()
    all_events = []

    logger.info(
        "Starting Wikimedia collection: pages=%s granularity=%s "
        "start=%s end=%s",
        len(pages),
        granularity,
        start,
        end,
    )

    with create_session() as session:
        for page in pages:
            try:
                events = collect_pageviews(
                    page_title=page,
                    start=start,
                    end=end,
                    project=project,
                    granularity=granularity,
                    session=session,
                )

                all_events.extend(events)

                logger.info(
                    "Collected %s events for page=%s",
                    len(events),
                    page,
                )

            except (requests.RequestException, ValueError, KeyError):
                logger.exception(
                    "Collection failed for page=%s",
                    page,
                )

    logger.info(
        "Wikimedia total events collected: %s",
        len(all_events),
    )

    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result = collect_all()

    print(f"Total events collected: {len(result)}")

    for event in result[:5]:
        print(event)
