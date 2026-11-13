
from unittest.mock import Mock, patch

import pytest

from ingestion.collectors.google_trends_collector import (
    collect_trends,
    normalize_trend,
)


def make_rss_item():
    item = Mock()
    item.findtext.side_effect = lambda field: {
        "title": "Artificial Intelligence",
        "pubDate": "Fri, 13 Nov 2026 01:50:00 GMT",
        "link": "https://trends.google.com/trending/rss?geo=MA",
        "{https://trends.google.com/trending/rss}approx_traffic": "1000+",
    }.get(field)
    return item


def test_normalize_trend_returns_valid_event():
    event = normalize_trend(make_rss_item(), "MA")

    assert event["source"] == "google_trends"
    assert event["payload"]["geo"] == "MA"
    assert event["payload"]["title"] == "Artificial Intelligence"
    assert event["payload"]["approx_traffic"] == "1000+"
    assert event["event_id"].startswith("google_trends:MA:")


def test_normalize_trend_rejects_missing_title():
    item = Mock()
    item.findtext.side_effect = lambda field: {
        "title": None,
        "pubDate": "Fri, 13 Nov 2026 01:50:00 GMT",
    }.get(field)

    with pytest.raises(ValueError):
        normalize_trend(item, "MA")


@patch(
    "ingestion.collectors.google_trends_collector.requests.Session.get"
)
def test_collect_trends_returns_events(mock_get):
    response = Mock()
    response.content = b"""
    <rss version="2.0">
      <channel>
        <item>
          <title>Artificial Intelligence</title>
          <link>https://trends.google.com/example</link>
          <pubDate>Fri, 13 Nov 2026 01:50:00 GMT</pubDate>
          <ht:approx_traffic
            xmlns:ht="https://trends.google.com/trending/rss"
          >1000+</ht:approx_traffic>
        </item>
      </channel>
    </rss>
    """
    mock_get.return_value = response

    events = collect_trends("MA")

    assert len(events) == 1
    assert events[0]["payload"]["title"] == "Artificial Intelligence"
    mock_get.assert_called_once()


def test_collect_trends_rejects_invalid_geo():
    with pytest.raises(ValueError):
        collect_trends("MAR")
