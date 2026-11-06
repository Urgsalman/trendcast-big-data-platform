from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PageviewEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    source: Literal["wikimedia_pageviews"]
    project: str = Field(min_length=1)
    page_title: str = Field(min_length=1)
    timestamp: datetime
    granularity: Literal["hourly", "daily"]
    views: int = Field(ge=0)
    schema_version: Literal["1.0"]


def validate_event(event: dict) -> dict:
    validated = PageviewEvent.model_validate(event)
    return validated.model_dump(mode="json")