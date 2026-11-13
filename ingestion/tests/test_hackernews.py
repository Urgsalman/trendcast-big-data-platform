
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from ingestion.collectors.hackernews_collector import (
    collect_stories,
    get_story,
    get_story_ids,
    normalize_story,
)


def make_response(data):
    response = Mock()
    response.json.return_value = data
    response.raise_for_status.return_value = None
    return response


def make_story(
    story_id=123,
    title="Test Big Data",
    story_type="story",
):
    return {
        "id": story_id,
        "type": story_type,
        "title": title,
        "time": 1791853200,
        "by": "test_user",
        "score": 100,
        "descendants": 25,
        "url": "https://example.com/article",
    }


def test_get_story_ids_returns_ids():
    session = Mock()
    session.get.return_value = make_response([123, 456, 789])

    result = get_story_ids(session, "topstories")

    assert result == [123, 456, 789]
    session.get.assert_called_once()


def test_get_story_ids_rejects_invalid_type():
    session = Mock()

    with pytest.raises(ValueError):
        get_story_ids(session, "invalid_type")

    session.get.assert_not_called()


def test_get_story_returns_valid_item():
    session = Mock()
    session.get.return_value = make_response(make_story())

    result = get_story(session, 123)

    assert result["id"] == 123
    assert result["title"] == "Test Big Data"


def test_get_story_ignores_deleted_item():
    session = Mock()

    deleted_story = make_story()
    deleted_story["deleted"] = True

    session.get.return_value = make_response(deleted_story)

    result = get_story(session, 123)

    assert result is None


def test_normalize_story_returns_valid_event():
    item = make_story()

    event = normalize_story(item)

    assert event["event_id"] == "hackernews:123"
    assert event["source"] == "hackernews"
    assert event["schema_version"] == "1.0"

    assert event["payload"]["story_id"] == 123
    assert event["payload"]["title"] == "Test Big Data"
    assert event["payload"]["score"] == 100
    assert event["payload"]["comments_count"] == 25

    timestamp = datetime.fromtimestamp(
        item["time"],
        tz=timezone.utc,
    )

    assert event["timestamp"] == timestamp.isoformat()


@patch(
    "ingestion.collectors.hackernews_collector.create_session"
)
def test_collect_stories_returns_events(mock_create_session):
    session = Mock()
    session.get.side_effect = [
        make_response([123, 456]),
        make_response(make_story(123)),
    ]

    mock_create_session.return_value = session

    events = collect_stories(
        limit=1,
        story_type="topstories",
    )

    assert len(events) == 1
    assert events[0]["event_id"] == "hackernews:123"

    session.close.assert_called_once()


@patch(
    "ingestion.collectors.hackernews_collector.create_session"
)
def test_collect_stories_returns_empty_list(mock_create_session):
    session = Mock()
    session.get.return_value = make_response([])

    mock_create_session.return_value = session

    events = collect_stories(limit=10)

    assert events == []
    session.close.assert_called_once()


def test_collect_stories_rejects_invalid_limit():
    with pytest.raises(ValueError):
        collect_stories(limit=0)


def test_collect_stories_rejects_invalid_story_type():
    with pytest.raises(ValueError):
        collect_stories(story_type="invalid_type")
