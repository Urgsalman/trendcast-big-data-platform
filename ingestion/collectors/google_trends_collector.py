
import hashlib
import logging
import os
import xml.etree.ElementTree as ET
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://trends.google.com/trending/rss"
GOOGLE_TRENDS_NAMESPACE = "https://trends.google.com/trending/rss"


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


def extract_geo_from_url(url: str | None) -> str | None:
    """Extract a two-letter geo parameter from a Google Trends URL."""
    if not url:
        return None

    try:
        query = parse_qs(urlparse(url).query)
        values = query.get("geo", [])

        if not values:
            return None

        candidate = values[0].strip().upper()

        if len(candidate) == 2 and candidate.isalpha():
            return candidate

    except (TypeError, ValueError):
        logger.warning("Could not parse Google Trends URL: %s", url)

    return None


def determine_feed_geo(
    requested_geo: str,
    response_url: str,
    items: list[ET.Element],
) -> str:
    """
    Determine the actual geography indicated by the RSS response.

    An empty requested geo is not sufficient proof that the feed
    is worldwide. Redirect URLs and item links are inspected.
    """
    if requested_geo:
        return requested_geo

    response_geo = extract_geo_from_url(response_url)

    if response_geo:
        logger.warning(
            "Google Trends feed redirected to geo=%s",
            response_geo,
        )
        return response_geo

    observed_geos = set()

    for item in items:
        geo = extract_geo_from_url(item.findtext("link"))
        if geo:
            observed_geos.add(geo)

    if len(observed_geos) == 1:
        detected_geo = next(iter(observed_geos))
        logger.warning(
            "Google Trends item links indicate geo=%s, not worldwide",
            detected_geo,
        )
        return detected_geo

    if len(observed_geos) > 1:
        logger.warning(
            "Google Trends item links contain multiple geographies: %s",
            sorted(observed_geos),
        )
        return "MIXED"

    logger.warning(
        "Google Trends response contains no verifiable geo parameter. "
        "Using WORLDWIDE as the requested feed scope, not as a "
        "verified global-coverage guarantee."
    )

    return "WORLDWIDE"


def normalize_trend(item: ET.Element, geo: str) -> dict:
    """Convert one RSS item into a TrendCast event."""
    title = item.findtext("title")

    if not title or not title.strip():
        raise ValueError("Google Trends item has no title")

    title = title.strip()

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

    event_id_source = (
        f"{geo}:{title}:{timestamp.isoformat()}"
    )

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
            "title": title,
            "approx_traffic": approx_traffic,
            "trend_url": trend_url,
        },
    }


def collect_trends(
    geo: str | None = None,
    existing_event_ids: set[str] | None = None,
) -> list[dict]:
    """
    Collect current Google Trends RSS items.

    Set GOOGLE_TRENDS_GEO to a two-letter country code for a
    country-specific feed. Leave it empty to request the default
    feed, whose actual geographical coverage is inspected.
    """
    if geo is None:
        geo = os.getenv("GOOGLE_TRENDS_GEO", "").strip().upper()
    else:
        geo = geo.strip().upper()

    if geo and (len(geo) != 2 or not geo.isalpha()):
        raise ValueError(
            "GOOGLE_TRENDS_GEO must be empty or a two-letter country code"
        )

    known_ids = set(existing_event_ids or set())
    session = create_session()

    try:
        params = {"geo": geo} if geo else {}

        response = session.get(
            BASE_URL,
            params=params,
            headers={
                "User-Agent": "TrendCast-BigData-StudentProject/1.0"
            },
            timeout=30,
        )

        response.raise_for_status()

        root = ET.fromstring(response.content)
        channel = root.find("channel")

        if channel is None:
            raise ValueError("Invalid Google Trends RSS response")

        items = channel.findall("item")

        actual_geo = determine_feed_geo(
            requested_geo=geo,
            response_url=response.url,
            items=items,
        )

        events_by_id = {}

        for item in items:
            try:
                event = normalize_trend(item, actual_geo)
                event_id = event["event_id"]

                if event_id in known_ids:
                    continue

                if event_id in events_by_id:
                    continue

                events_by_id[event_id] = event

            except (ValueError, TypeError, KeyError):
                logger.exception(
                    "Failed to normalize a Google Trends item"
                )

        events = list(events_by_id.values())

        logger.info(
            "Collected %s new Google Trends events for %s",
            len(events),
            actual_geo,
        )

        return events

    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collected_events = collect_trends()

    print(f"Total new unique events collected: {len(collected_events)}")

    for event in collected_events[:5]:
        print(event)
