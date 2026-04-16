"""Pydantic-Schemas für die Management-API."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class SourceCreate(BaseModel):
    name: str = Field(..., max_length=255)
    system_url: HttpUrl
    is_active: bool = False
    sync_interval_min: int = Field(10, ge=1, le=1440)
    config: dict[str, Any] = Field(default_factory=dict)


class SourceUpdate(BaseModel):
    name: str | None = None
    is_active: bool | None = None
    sync_interval_min: int | None = Field(None, ge=1, le=1440)
    config: dict[str, Any] | None = None


class SourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    system_url: str
    is_active: bool
    sync_interval_min: int
    last_sync_at: datetime | None
    last_full_sync_at: datetime | None
    last_success_at: datetime | None
    last_error: str | None
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class SyncLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_id: UUID | None
    flow_run_id: str | None
    sync_type: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_seconds: float | None
    entities_synced: dict[str, Any]
    errors: list[Any]
    triggered_by: str


class SyncRequest(BaseModel):
    full: bool = False


class SyncResponse(BaseModel):
    accepted: bool
    flow_run_id: str | None = None
    message: str


class StatsOut(BaseModel):
    sources_total: int
    sources_active: int
    bodies: int
    organizations: int
    persons: int
    meetings: int
    papers: int
    files: int
    files_with_text: int
