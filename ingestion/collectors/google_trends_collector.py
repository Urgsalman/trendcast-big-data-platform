
import hashlib
import logging
import os
import xml.etree.ElementTree as ET
from datetime import timezone
from email.utils import parsedate_to_datetime

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://trends.google.com/trending/rss"
GOOGLE_TRENDS_NAMESPACE = (
    "https://trends.google.com/trending/rss"
)


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


def normalize_trend(item: ET.Element, geo: str) -> dict:
    """Convert one RSS item into a TrendCast event."""
    title = item.findtext("title")

    if not title:
        raise ValueError("Google Trends item has no title")

    pub_date = item.findtext("pubDate")

    if not pub_date:
        raise ValueError("Google Trends item has no publication date")

    timestamp = parsedate_to_datetime(pub_date)

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    timestamp = timestamp.astimezone(timezone.utc)

    approx_traffic = item.findtext(
        f"{{{GOOGLE_TRENDS_NAMESPACE}}}approx_traffic"
    )

    trend_url = item.findtext("link")

    event_id_source = f"{geo}:{title}:{timestamp.isoformat()}"
    event_hash = hashlib.sha256(
        event_id_source.encode("utf-8")
    ).hexdigest()[:16]

    return {
        "event_id": f"google_trends:{geo}:{event_hash}",
        "source": "google_trends",
        "timestamp": timestamp.isoformat(),
        "schema_version": "1.0",
        "payload": {
            "geo": geo,
            "title": title.strip(),
            "approx_traffic": approx_traffic,
            "trend_url": trend_url,
        },
    }


def collect_trends(geo: str | None = None) -> list[dict]:
    """Collect current Google Trends for a country."""
    geo = (geo or os.getenv("GOOGLE_TRENDS_GEO", "MA")).strip().upper()

    if len(geo) != 2 or not geo.isalpha():
        raise ValueError("geo must be a two-letter country code")

    session = create_session()

    try:
        response = session.get(
            BASE_URL,
            params={"geo": geo},
            headers={"User-Agent": "TrendCast-BigData-StudentProject/1.0"},
            timeout=30,
        )
        response.raise_for_status()

        root = ET.fromstring(response.content)
        channel = root.find("channel")

        if channel is None:
            raise ValueError("Invalid Google Trends RSS response")

        events = []

        for item in channel.findall("item"):
            try:
                events.append(normalize_trend(item, geo))
            except (ValueError, TypeError):
                logger.exception("Failed to normalize a Google Trends item")

        logger.info(
            "Collected %s Google Trends events for %s",
            len(events),
            geo,
        )

        return events

    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collected_events = collect_trends()

    print(f"Total events collected: {len(collected_events)}")

    for event in collected_events[:5]:
        print(event)
