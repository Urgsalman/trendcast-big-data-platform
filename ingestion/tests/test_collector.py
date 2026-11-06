from unittest.mock import Mock, patch

import pytest
import requests

from ingestion.collectors.wikimedia_collector import collect_pageviews


@patch("ingestion.collectors.wikimedia_collector.requests.get")
def test_collect_pageviews_returns_valid_events(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {
        "items": [
            {"timestamp": "2026100100", "views": 5020},
            {"timestamp": "2026100200", "views": 5325},
        ]
    }
    mock_get.return_value = mock_response

    events = collect_pageviews(
        page_title="Python_(programming_language)",
        start="2026100100",
        end="2026100200",
    )

    assert len(events) == 2
    assert events[0]["source"] == "wikimedia_pageviews"
    assert events[0]["views"] == 5020
    assert events[0]["page_title"] == "Python_(programming_language)"
    assert events[0]["event_id"].endswith("2026100100")
    assert events[0]["timestamp"] == "2026-10-01T00:00:00+00:00"


@patch("ingestion.collectors.wikimedia_collector.requests.get")
def test_collect_pageviews_returns_empty_list(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {"items": []}
    mock_get.return_value = mock_response

    events = collect_pageviews(
        page_title="Python_(programming_language)",
        start="2026100100",
        end="2026100200",
    )

    assert events == []


@patch("ingestion.collectors.wikimedia_collector.requests.get")
def test_collect_pageviews_raises_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP 500")
    mock_get.return_value = mock_response

    with pytest.raises(requests.HTTPError):
        collect_pageviews(
            page_title="Python_(programming_language)",
            start="2026100100",
            end="2026100200",
        )