
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class WikimediaPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: str = Field(min_length=1)
    page_title: str = Field(min_length=1)
    granularity: Literal["hourly", "daily"]
    views: int = Field(ge=0)


class HackerNewsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    story_id: int = Field(gt=0)
    story_type: str = Field(min_length=1)
    title: str = Field(min_length=1)
    url: str | None = None
    discussion_url: str = Field(min_length=1)
    author: str | None = None
    score: int = Field(ge=0)
    comments_count: int = Field(ge=0)


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    source: Literal[
    "wikimedia_pageviews",
    "hackernews",
    "google_trends",
]
    timestamp: datetime
    schema_version: Literal["1.0"]
    payload: dict

class GoogleTrendsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    geo: str = Field(min_length=2, max_length=2, pattern=r"^[A-Z]{2}$")
    title: str = Field(min_length=1)
    approx_traffic: str | None = None
    trend_url: str | None = None


def validate_event(event: dict) -> dict:
    envelope = EventEnvelope.model_validate(event)

    if envelope.source == "wikimedia_pageviews":
        payload = WikimediaPayload.model_validate(envelope.payload)
    elif envelope.source == "hackernews":
        payload = HackerNewsPayload.model_validate(envelope.payload)
    elif envelope.source == "google_trends":
        payload = GoogleTrendsPayload.model_validate(envelope.payload)
    else:
        raise ValueError(f"Unsupported event source: {envelope.source}")

    validated_event = envelope.model_copy(
        update={"payload": payload.model_dump()}
    )

    return validated_event.model_dump(mode="json")
