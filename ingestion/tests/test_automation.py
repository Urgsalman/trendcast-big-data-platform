"""Offline tests for incremental collection, broker failures and restart recovery."""

from datetime import date, datetime, timedelta, timezone
from threading import Event, Thread
from unittest.mock import Mock

import pytest

from ingestion import run_automation as automation
from ingestion.automation_state import PublicationState

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)


def pageview(page="Python", day="2026111800"):
    return {
        "event_id": f"en.wikipedia:{page}:{day}",
        "source": "wikimedia_pageviews",
        "timestamp": datetime.strptime(day, "%Y%m%d%H")
        .replace(tzinfo=timezone.utc)
        .isoformat(),
        "schema_version": "1.0",
        "payload": {
            "project": "en.wikipedia",
            "page_title": page,
            "granularity": "daily",
            "views": 42,
        },
    }


def story():
    return {
        "event_id": "hackernews:123",
        "source": "hackernews",
        "timestamp": "2026-11-18T12:00:00+00:00",
        "schema_version": "1.0",
        "payload": {
            "story_id": 123,
            "story_type": "story",
            "title": "Python",
            "discussion_url": "https://news.ycombinator.com/item?id=123",
            "score": 10,
            "comments_count": 2,
        },
    }


@pytest.fixture
def state(tmp_path):
    db = PublicationState(str(tmp_path / "state.sqlite3"))
    yield db
    db.close()


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    monkeypatch.setenv("WIKIMEDIA_GRANULARITY", "daily")
    monkeypatch.setenv("WIKIMEDIA_DAYS_BACK", "3")
    monkeypatch.setenv("WIKIMEDIA_PAGES", "Python")
    monkeypatch.setenv("WIKIMEDIA_PROJECT", "en.wikipedia")
    monkeypatch.setenv("KAFKA_TOPIC", "test.events")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    for name in (
        "WIKIMEDIA_START",
        "WIKIMEDIA_END",
        *automation.INTERVAL_VARIABLES.values(),
    ):
        monkeypatch.delenv(name, raising=False)


def test_confirmed_event_is_not_republished_after_restart(tmp_path):
    path = str(tmp_path / "state.sqlite3")
    producer = Mock()
    first = PublicationState(path)
    automation.AutomationRunner(first, lambda: producer).publish(pageview())
    first.close()
    second = PublicationState(path)
    try:
        runner = automation.AutomationRunner(second, lambda: producer)
        runner.publish(pageview())
        assert producer.publish.call_count == 1
        assert runner.skipped == 1
    finally:
        second.close()


def test_publication_failure_remains_retryable(state):
    producer = Mock()
    producer.publish.side_effect = RuntimeError("broker unavailable")
    runner = automation.AutomationRunner(state, lambda: producer)
    runner.publish(pageview())
    assert runner.failed
    producer.publish.side_effect = None
    runner.publish(pageview())
    assert runner.published == 1
    assert producer.publish.call_count == 2


def test_invalid_event_never_opens_kafka(state):
    factory = Mock()
    runner = automation.AutomationRunner(state, factory)
    runner.publish({"event_id": "bad"})
    assert runner.failed
    factory.assert_not_called()


def test_receipts_are_separate_for_each_kafka_destination(state, monkeypatch):
    producer = Mock()
    automation.AutomationRunner(state, lambda: producer).publish(pageview())
    monkeypatch.setenv("KAFKA_TOPIC", "other.events")
    automation.AutomationRunner(state, lambda: producer).publish(pageview())
    assert producer.publish.call_count == 2


def test_initial_history_then_only_new_day(state, monkeypatch):
    collect = Mock(return_value=[pageview(day=f"202611{d}00") for d in (16, 17, 18)])
    monkeypatch.setattr(automation, "collect_pageviews", collect)
    runner = automation.AutomationRunner(state, Mock)
    assert runner.run_source("wikimedia_pageviews", NOW)
    assert collect.call_args.args[1:] == ("2026111600", "2026111800")
    collect.reset_mock()
    assert runner.run_source("wikimedia_pageviews", NOW)
    collect.assert_not_called()
    collect.return_value = [pageview(day="2026111900")]
    assert runner.run_source("wikimedia_pageviews", NOW + timedelta(days=1))
    assert collect.call_args.args[1:] == ("2026111900", "2026111900")


def test_partial_history_retries_the_missing_day_only(state, monkeypatch):
    collect = Mock(return_value=[pageview(day=f"202611{d}00") for d in (16, 18)])
    monkeypatch.setattr(automation, "collect_pageviews", collect)
    runner = automation.AutomationRunner(state, Mock)
    assert not runner.run_source("wikimedia_pageviews", NOW)
    collect.return_value = [pageview(day="2026111700")]
    assert runner.run_source("wikimedia_pageviews", NOW)
    assert collect.call_args.args[1:] == ("2026111700", "2026111700")


def test_empty_history_is_retried_and_long_downtime_keeps_original_start(
    state, monkeypatch
):
    collect = Mock(return_value=[])
    monkeypatch.setattr(automation, "collect_pageviews", collect)
    runner = automation.AutomationRunner(state, Mock)
    assert not runner.run_source("wikimedia_pageviews", NOW)
    assert not runner.run_source("wikimedia_pageviews", NOW + timedelta(days=100))
    assert collect.call_args.args[1] == "2026111600"


def test_one_failed_page_does_not_block_the_next(state, monkeypatch):
    monkeypatch.setenv("WIKIMEDIA_PAGES", "Broken,Python")
    monkeypatch.setenv("WIKIMEDIA_DAYS_BACK", "1")
    collect = Mock(side_effect=[RuntimeError("API failure"), [pageview()]])
    monkeypatch.setattr(automation, "collect_pageviews", collect)
    runner = automation.AutomationRunner(state, Mock)
    assert not runner.run_source("wikimedia_pageviews", NOW)
    assert runner.published == 1
    assert collect.call_count == 2


def test_snapshot_deduplicates_same_slot_but_keeps_next_observation(state, monkeypatch):
    monkeypatch.setattr(automation, "collect_stories", lambda: [story()])
    producer = Mock()
    runner = automation.AutomationRunner(state, lambda: producer)
    assert runner.run_source("hackernews", NOW)
    assert runner.run_source("hackernews", NOW + timedelta(minutes=1))
    assert producer.publish.call_count == 1
    assert runner.run_source("hackernews", NOW + timedelta(minutes=30))
    assert producer.publish.call_count == 2
    # Source identifiers and timestamps remain unchanged for existing consumers.
    assert producer.publish.call_args.args[0]["event_id"] == "hackernews:123"
    assert datetime.fromisoformat(
        producer.publish.call_args.args[0]["timestamp"]
    ) == datetime.fromisoformat(story()["timestamp"])


def test_collector_failure_closes_no_unopened_producer(state, monkeypatch):
    monkeypatch.setattr(
        automation, "collect_trends", Mock(side_effect=ValueError("bad RSS"))
    )
    factory = Mock()
    runner = automation.AutomationRunner(state, factory)
    assert not runner.run_source("google_trends", NOW)
    factory.assert_not_called()


def test_failed_publication_closes_producer(state, monkeypatch):
    monkeypatch.setattr(automation, "collect_stories", lambda: [story()])
    producer = Mock()
    producer.publish.side_effect = RuntimeError("send failed")
    runner = automation.AutomationRunner(state, lambda: producer)
    assert not runner.run_source("hackernews", NOW)
    producer.close.assert_called_once()


def test_missing_ranges_preserves_holes():
    assert automation.missing_ranges(
        date(2026, 11, 1), date(2026, 11, 5), lambda day: day.day in (2, 4)
    ) == [
        (date(2026, 11, 1), date(2026, 11, 1)),
        (date(2026, 11, 3), date(2026, 11, 3)),
        (date(2026, 11, 5), date(2026, 11, 5)),
    ]


@pytest.mark.parametrize("value", ["0", "-5", "bad"])
def test_scheduler_rejects_invalid_interval(monkeypatch, value):
    monkeypatch.setenv("HACKERNEWS_INTERVAL_SECONDS", value)
    with pytest.raises(ValueError):
        automation.build_scheduler("unused.sqlite3")


def test_scheduler_actually_triggers_all_three_sources(monkeypatch, tmp_path):
    seen = []
    finished = Event()

    def collect(source, path):
        seen.append(source)
        if len(seen) == 3:
            finished.set()

    monkeypatch.setattr(automation, "execute_source", collect)
    scheduler = automation.build_scheduler(str(tmp_path / "unused.sqlite3"))
    thread = Thread(target=scheduler.start, daemon=True)
    thread.start()
    try:
        assert finished.wait(timeout=5)
        assert set(seen) == set(automation.SOURCES)
    finally:
        scheduler.shutdown(wait=True)
        thread.join(timeout=5)
    assert not thread.is_alive()
