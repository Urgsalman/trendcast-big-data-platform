
import pytest
from pydantic import ValidationError

from ingestion.validators.event_validator import validate_event


def test_valid_event():
    event = {
        "event_id": "en.wikipedia:Python:20261001",
        "source": "wikimedia_pageviews",
        "project": "en.wikipedia",
        "page_title": "Python",
        "timestamp": "2026-10-01T00:00:00Z",
        "granularity": "daily",
        "views": 1000,
        "schema_version": "1.0",
    }

    result = validate_event(event)
    assert result["views"] == 1000


def test_negative_views_are_rejected():
    event = {
        "event_id": "test",
        "source": "wikimedia_pageviews",
        "project": "en.wikipedia",
        "page_title": "Python",
        "timestamp": "2026-10-01T00:00:00Z",
        "granularity": "daily",
        "views": -5,
        "schema_version": "1.0",
    }

    with pytest.raises(ValidationError):
        validate_event(event)


def test_missing_required_field_is_rejected():
    event = {
        "event_id": "test",
        "source": "wikimedia_pageviews",
        "project": "en.wikipedia",
        "page_title": "Python",
        "timestamp": "2026-10-01T00:00:00Z",
        "granularity": "daily",
        "views": 1000
    }

    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_timestamp_is_rejected():
    event = {
        "event_id": "test",
        "source": "wikimedia_pageviews",
        "project": "en.wikipedia",
        "page_title": "Python",
        "timestamp": "invalid-date",
        "granularity": "daily",
        "views": 1000,
        "schema_version": "1.0",
    }

    with pytest.raises(ValidationError):
        validate_event(event)


def test_extra_field_is_rejected():
    event = {
        "event_id": "test",
        "source": "wikimedia_pageviews",
        "project": "en.wikipedia",
        "page_title": "Python",
        "timestamp": "2026-10-01T00:00:00Z",
        "granularity": "daily",
        "views": 1000,
        "schema_version": "1.0",
        "unexpected_field": "invalid"
    }

    with pytest.raises(ValidationError):
        validate_event(event)
