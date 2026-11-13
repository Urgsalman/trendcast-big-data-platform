
import pytest
from pydantic import ValidationError

from ingestion.validators.event_validator import validate_event


def make_wikimedia_event():
    return {
        "event_id": "en.wikipedia:Python:2026100100",
        "source": "wikimedia_pageviews",
        "timestamp": "2026-10-01T00:00:00Z",
        "schema_version": "1.0",
        "payload": {
            "project": "en.wikipedia",
            "page_title": "Python",
            "granularity": "daily",
            "views": 1000,
        },
    }


def make_hackernews_event():
    return {
        "event_id": "hackernews:123456",
        "source": "hackernews",
        "timestamp": "2026-10-01T10:30:00Z",
        "schema_version": "1.0",
        "payload": {
            "story_id": 123456,
            "story_type": "story",
            "title": "Example Hacker News story",
            "url": "https://example.com",
            "discussion_url": "https://news.ycombinator.com/item?id=123456",
            "author": "test_user",
            "score": 150,
            "comments_count": 42,
        },
    }


def test_valid_wikimedia_event():
    event = make_wikimedia_event()

    result = validate_event(event)

    assert result["source"] == "wikimedia_pageviews"
    assert result["payload"]["views"] == 1000
    assert result["payload"]["page_title"] == "Python"


def test_valid_hackernews_event():
    event = make_hackernews_event()

    result = validate_event(event)

    assert result["source"] == "hackernews"
    assert result["payload"]["story_id"] == 123456
    assert result["payload"]["score"] == 150


def test_negative_views_are_rejected():
    event = make_wikimedia_event()
    event["payload"]["views"] = -5

    with pytest.raises(ValidationError):
        validate_event(event)


def test_missing_required_field_is_rejected():
    event = make_wikimedia_event()
    del event["schema_version"]

    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_timestamp_is_rejected():
    event = make_wikimedia_event()
    event["timestamp"] = "invalid-date"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_extra_envelope_field_is_rejected():
    event = make_wikimedia_event()
    event["unexpected_field"] = "invalid"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_source_is_rejected():
    event = make_wikimedia_event()
    event["source"] = "unknown_source"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_granularity_is_rejected():
    event = make_wikimedia_event()
    event["payload"]["granularity"] = "monthly"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_negative_hackernews_score_is_rejected():
    event = make_hackernews_event()
    event["payload"]["score"] = -10

    with pytest.raises(ValidationError):
        validate_event(event)


def test_extra_payload_field_is_rejected():
    event = make_wikimedia_event()
    event["payload"]["unexpected_field"] = "invalid"

    with pytest.raises(ValidationError):
        validate_event(event)

def make_google_trends_event():
    return {
        "event_id": "google_trends:MA:123456789",
        "source": "google_trends",
        "timestamp": "2026-11-13T01:50:00Z",
        "schema_version": "1.0",
        "payload": {
            "geo": "MA",
            "title": "intelligence artificielle",
            "approx_traffic": "1000+",
            "trend_url": "https://trends.google.com/trending/rss?geo=MA",
        },
    }


def test_valid_google_trends_event():
    event = make_google_trends_event()

    result = validate_event(event)

    assert result["source"] == "google_trends"
    assert result["payload"]["geo"] == "MA"
    assert result["payload"]["title"] == "intelligence artificielle"


def test_invalid_google_trends_geo_is_rejected():
    event = make_google_trends_event()
    event["payload"]["geo"] = "MOROCCO"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_empty_google_trends_title_is_rejected():
    event = make_google_trends_event()
    event["payload"]["title"] = ""

    with pytest.raises(ValidationError):
        validate_event(event)


def test_extra_google_trends_payload_field_is_rejected():
    event = make_google_trends_event()
    event["payload"]["unexpected"] = "invalid"

    with pytest.raises(ValidationError):
        validate_event(event)