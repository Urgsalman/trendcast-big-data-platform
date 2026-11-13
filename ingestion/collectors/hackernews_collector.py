
import logging
import os
from datetime import datetime, timezone
from html import unescape

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://hacker-news.firebaseio.com/v0"

ALLOWED_STORY_TYPES = {
    "topstories",
    "newstories",
    "beststories",
    "askstories",
    "showstories",
}


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


def get_story_ids(
    session: requests.Session,
    story_type: str,
) -> list[int]:
    """Retrieve story IDs from the Hacker News API."""
    if story_type not in ALLOWED_STORY_TYPES:
        raise ValueError(
            f"Unsupported story type: {story_type}"
        )

    url = f"{BASE_URL}/{story_type}.json"

    response = session.get(url, timeout=30)
    response.raise_for_status()

    story_ids = response.json()

    if not isinstance(story_ids, list):
        raise TypeError("Invalid Hacker News story ID response")

    return story_ids


def get_story(
    session: requests.Session,
    story_id: int,
) -> dict | None:
    """Retrieve one Hacker News story."""
    url = f"{BASE_URL}/item/{story_id}.json"

    response = session.get(url, timeout=30)
    response.raise_for_status()

    item = response.json()

    if not item or item.get("deleted") or item.get("dead"):
        return None

    if item.get("type") not in {"story", "job", "poll"}:
        return None

    if not item.get("title") or not item.get("time"):
        return None

    return item


def normalize_story(item: dict) -> dict:
    """Convert a Hacker News item into a TrendCast event."""
    story_id = item["id"]

    timestamp = datetime.fromtimestamp(
        item["time"],
        tz=timezone.utc,
    )

    title = unescape(item["title"])

    return {
        "event_id": f"hackernews:{story_id}",
        "source": "hackernews",
        "timestamp": timestamp.isoformat(),
        "schema_version": "1.0",
        "payload": {
            "story_id": story_id,
            "story_type": item["type"],
            "title": title,
            "url": item.get("url"),
            "discussion_url": (
                f"https://news.ycombinator.com/item?id={story_id}"
            ),
            "author": item.get("by"),
            "score": item.get("score", 0),
            "comments_count": item.get("descendants", 0),
        },
    }


def collect_stories(limit=None, story_type=None):
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
    ):
        raise ValueError("limit must be a positive integer")

    if limit is None:
        limit = int(os.getenv("HACKERNEWS_LIMIT", "30"))

    if story_type is None:
        story_type = os.getenv("HACKERNEWS_STORY_TYPE", "topstories")

    session = create_session()
    events = []

    try:
        story_ids = get_story_ids(session, story_type)

        logger.info(
            "Retrieved %s IDs from Hacker News (%s)",
            len(story_ids),
            story_type,
        )

        for story_id in story_ids[:limit]:
            try:
                item = get_story(session, story_id)

                if item is None:
                    continue

                event = normalize_story(item)
                events.append(event)

            except (requests.RequestException, ValueError, KeyError):
                logger.exception(
                    "Failed to collect Hacker News story %s",
                    story_id,
                )

    finally:
        session.close()

    logger.info(
        "Collected %s Hacker News events",
        len(events),
    )

    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collected_events = collect_stories()

    print(f"Total events collected: {len(collected_events)}")

    for event in collected_events[:5]:
        print(event)
