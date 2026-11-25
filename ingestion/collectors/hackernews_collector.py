
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
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


def get_story_ids(session: requests.Session, story_type: str) -> list:
    if story_type not in ALLOWED_STORY_TYPES:
        raise ValueError(
            f"Unsupported Hacker News story type: {story_type}"
        )

    response = session.get(
        f"{BASE_URL}/{story_type}.json",
        timeout=30,
    )
    response.raise_for_status()

    story_ids = response.json()

    if not isinstance(story_ids, list):
        raise TypeError("Invalid Hacker News story ID response")

    return story_ids


def get_story(session: requests.Session, story_id: int):
    response = session.get(
        f"{BASE_URL}/item/{story_id}.json",
        timeout=30,
    )
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
    story_id = item["id"]

    timestamp = datetime.fromtimestamp(
        item["time"],
        tz=timezone.utc,
    )

    return {
        "event_id": f"hackernews:{story_id}",
        "source": "hackernews",
        "timestamp": timestamp.isoformat(),
        "schema_version": "1.0",
        "payload": {
            "story_id": story_id,
            "story_type": item["type"],
            "title": unescape(item["title"]),
            "url": item.get("url"),
            "discussion_url": (
                f"https://news.ycombinator.com/item?id={story_id}"
            ),
            "author": item.get("by"),
            "score": item.get("score", 0),
            "comments_count": item.get("descendants", 0),
        },
    }


def get_configured_story_types() -> list[str]:
    configured_types = os.getenv(
        "HACKERNEWS_STORY_TYPES",
        "topstories,newstories,beststories,askstories,showstories",
    )

    story_types = [
        value.strip().lower()
        for value in configured_types.split(",")
        if value.strip()
    ]

    if not story_types:
        raise ValueError(
            "HACKERNEWS_STORY_TYPES must contain at least one category"
        )

    invalid_types = set(story_types) - ALLOWED_STORY_TYPES

    if invalid_types:
        raise ValueError(
            f"Unsupported story types: {sorted(invalid_types)}"
        )

    # Preserve order while removing duplicate category names.
    return list(dict.fromkeys(story_types))


def collect_stories(
    limit: int | None = None,
    story_type: str | None = None,
    existing_event_ids: set[str] | None = None,
) -> list[dict]:
    """
    Collect Hacker News stories.

    limit is applied independently to each selected category.
    existing_event_ids contains IDs already published to Kafka.
    """
    if limit is None:
        limit = int(os.getenv("HACKERNEWS_LIMIT", "500"))

    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or limit <= 0
    ):
        raise ValueError("limit must be a positive integer")

    if story_type is not None:
        story_types = [story_type.strip().lower()]

        invalid_types = set(story_types) - ALLOWED_STORY_TYPES
        if invalid_types:
            raise ValueError(
                f"Unsupported story types: {sorted(invalid_types)}"
            )
    else:
        story_types = get_configured_story_types()

    known_ids = set(existing_event_ids or set())
    events_by_id = {}

    session = create_session()

    try:
        for current_type in story_types:
            try:
                story_ids = get_story_ids(session, current_type)
                selected_ids = story_ids[:limit]

                logger.info(
                    "Hacker News %s: retrieved %s IDs; processing %s",
                    current_type,
                    len(story_ids),
                    len(selected_ids),
                )

                for story_id in selected_ids:
                    event_id = f"hackernews:{story_id}"

                    if event_id in known_ids:
                        continue

                    if event_id in events_by_id:
                        continue

                    try:
                        item = get_story(session, story_id)

                        if item is None:
                            continue

                        event = normalize_story(item)
                        events_by_id[event["event_id"]] = event

                    except (
                        requests.RequestException,
                        ValueError,
                        TypeError,
                        KeyError,
                    ):
                        logger.exception(
                            "Failed to collect Hacker News story %s",
                            story_id,
                        )

            except requests.RequestException:
                logger.exception(
                    "Failed to retrieve Hacker News category %s",
                    current_type,
                )

    finally:
        session.close()

    events = list(events_by_id.values())

    logger.info(
        "Collected %s new unique Hacker News events across %s categories",
        len(events),
        len(story_types),
    )

    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collected_events = collect_stories()

    print(f"Total new unique events collected: {len(collected_events)}")

    for event in collected_events[:5]:
        print(event)
