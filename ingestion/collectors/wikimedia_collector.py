
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

HEADERS = {
    "User-Agent": "TrendCast-BigData-StudentProject/1.0 (contact: soulaimaine.ch@gmail.com)"
}

PAGES = [
    "Python_(programming_language)",
    "Artificial_intelligence",
    "Big_data",
    "Machine_learning",
    "Cloud_computing",
]


def collect_pageviews(
    page_title: str,
    start: str,
    end: str,
    project: str = "en.wikipedia",
) -> list[dict]:
    url = (
        f"{BASE_URL}/{project}/all-access/user/"
        f"{page_title}/daily/{start}/{end}"
    )

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    items = response.json().get("items", [])
    events = []

    for item in items:
        timestamp = datetime.strptime(
            item["timestamp"], "%Y%m%d%H"
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


def collect_all(
    start: str = "2026100100",
    end: str = "2026100700",
) -> list[dict]:
    all_events = []

    for page in PAGES:
        try:
            events = collect_pageviews(page, start, end)
            all_events.extend(events)

            logger.info(
                "Collected %s events for %s",
                len(events),
                page,
            )

        except requests.RequestException:
            logger.exception(
                "Collection failed for %s",
                page,
            )

    return all_events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result = collect_all()

    print(f"Total events collected: {len(result)}")

    for event in result[:5]:
        print(event)
