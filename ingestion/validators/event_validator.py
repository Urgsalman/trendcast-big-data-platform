from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PageviewEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    source: str
    project: str
    page_title: str
    timestamp: datetime
    granularity: str
    views: int = Field(ge=0)
    schema_version: str


def validate_event(event: dict) -> dict:
    validated = PageviewEvent.model_validate(event)
    return validated.model_dump(mode="json")