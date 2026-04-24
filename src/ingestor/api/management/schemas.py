"""Pydantic-Schemas für die Management-API."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

VALID_SCOPES = {"read", "write", "admin"}


# =============================================================================
# Sources
# =============================================================================


class SourceCreate(BaseModel):
    name: str = Field(..., max_length=255, min_length=1)
    system_url: HttpUrl
    is_active: bool = False
    sync_interval_min: int = Field(10, ge=1, le=1440)
    config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("system_url")
    @classmethod
    def _only_http_https(cls, v: HttpUrl) -> HttpUrl:
        """SSRF-Schutz: Nur http/https erlauben (kein file://, ftp://, gopher:// etc.)."""
        if v.scheme not in ("http", "https"):
            raise ValueError("system_url muss http:// oder https:// sein")
        return v


class SourceUpdate(BaseModel):
    name: str | None = Field(None, max_length=255, min_length=1)
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
    consecutive_failures: int
    quarantined_at: datetime | None
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class SourceStatsOut(BaseModel):
    """Pro-Source-Metriken."""

    source_id: UUID
    name: str
    is_active: bool
    bodies: int
    organizations: int
    persons: int
    meetings: int
    papers: int
    files: int
    files_with_text: int
    files_pending: int
    files_failed: int
    last_sync_at: datetime | None
    last_success_at: datetime | None
    last_sync_duration_seconds: float | None
    consecutive_failures: int
    quarantined_at: datetime | None


class BulkSourceIdsRequest(BaseModel):
    source_ids: list[UUID] = Field(..., min_length=1, max_length=500)


class BulkActionResult(BaseModel):
    affected: int
    not_found: list[UUID] = Field(default_factory=list)


class CsvImportResult(BaseModel):
    inserted: int
    skipped_duplicate: int
    invalid_rows: int


# =============================================================================
# Sync
# =============================================================================


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


# =============================================================================
# Stats
# =============================================================================


class StatsOut(BaseModel):
    sources_total: int
    sources_active: int
    sources_quarantined: int
    bodies: int
    organizations: int
    persons: int
    meetings: int
    papers: int
    files: int
    files_with_text: int
    files_pending: int
    files_failed: int


# =============================================================================
# Files / OCR
# =============================================================================


class FileOcrRetryResult(BaseModel):
    file_id: UUID
    previous_status: str
    new_status: Literal["pending"]


class BulkOcrRetryResult(BaseModel):
    reset_count: int


class EnrichmentResult(BaseModel):
    body_id: UUID
    photos_tried: int = 0
    photos_success: int = 0
    geocode_tried: int = 0
    geocode_success: int = 0


# =============================================================================
# API-Keys
# =============================================================================


class ApiKeyCreate(BaseModel):
    name: str = Field(..., max_length=100, min_length=1)
    scopes: list[Literal["read", "write", "admin"]] = Field(default_factory=lambda: ["read"])
    expires_at: datetime | None = None

    @field_validator("scopes")
    @classmethod
    def _unique_scopes(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("mindestens ein Scope erforderlich")
        return sorted(set(v))


class ApiKeyOut(BaseModel):
    """Ohne Klartext-Key — der wird nur bei POST zurückgegeben."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    prefix: str
    scopes: list[str]
    created_by: str | None
    created_at: datetime
    last_used_at: datetime | None
    last_used_ip: str | None
    revoked_at: datetime | None
    expires_at: datetime | None

    @classmethod
    def from_model(cls, m) -> "ApiKeyOut":
        return cls(
            id=m.id,
            name=m.name,
            prefix=m.prefix,
            scopes=[s.strip() for s in m.scopes.split(",") if s.strip()],
            created_by=m.created_by,
            created_at=m.created_at,
            last_used_at=m.last_used_at,
            last_used_ip=m.last_used_ip,
            revoked_at=m.revoked_at,
            expires_at=m.expires_at,
        )


class ApiKeyCreateResponse(BaseModel):
    """Enthält den Klartext-Key — wird NUR einmal zurückgegeben."""

    key: ApiKeyOut
    plaintext_key: str = Field(
        ...,
        description="Der Klartext-Key — bitte sofort sicher speichern. Kann nicht erneut abgerufen werden.",
    )


# =============================================================================
# Audit-Logs
# =============================================================================


class AuditLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    timestamp: datetime
    api_key_id: UUID | None
    actor: str
    action: str
    resource_type: str | None
    resource_id: str | None
    ip_address: str | None
    status_code: int | None
    details: dict[str, Any]
