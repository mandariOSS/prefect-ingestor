"""
Management-API: Sources, Sync, Stats, API-Keys, Audit-Log, OCR-Control,
Enrichment-Trigger.

Auth: X-API-Key Header. Jede Route deklariert ihren benötigten Scope via
RequireRead/RequireWrite/RequireAdmin Dependencies. Aktionen werden in
audit_logs protokolliert.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from ingestor.api.auth import (
    RequireAdmin,
    RequireRead,
    RequireWrite,
    authenticate,
    generate_api_key,
    hash_api_key,
    key_prefix,
    write_audit_log,
)
from ingestor.api.dependencies import SessionDep
from ingestor.api.management.schemas import (
    ApiKeyCreate,
    ApiKeyCreateResponse,
    ApiKeyOut,
    AuditLogOut,
    BulkActionResult,
    BulkOcrRetryResult,
    BulkSourceIdsRequest,
    CsvImportResult,
    EnrichmentResult,
    FileOcrRetryResult,
    SourceCreate,
    SourceOut,
    SourceStatsOut,
    SourceUpdate,
    StatsOut,
    SyncLogOut,
    SyncRequest,
    SyncResponse,
)
from ingestor.db.models import (
    ApiKey,
    AuditLog,
    Body,
    File,
    Meeting,
    Organization,
    Paper,
    Person,
    Source,
    SyncLog,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["management"],
    # Globale Auth-Dependency: jede Route muss mindestens authentifiziert sein.
    # Fein-Scope-Checks erfolgen pro Route via RequireRead/Write/Admin.
    dependencies=[Depends(authenticate)],
)


# =============================================================================
# Sources — CRUD
# =============================================================================


@router.get("/sources", response_model=list[SourceOut])
async def list_sources(
    auth: RequireRead,
    session: SessionDep,
    active: bool | None = Query(None, description="Filter: is_active"),
    quarantined: bool | None = Query(None, description="Filter: quarantined_at IS NOT NULL"),
) -> list[Source]:
    stmt = select(Source).order_by(Source.name)
    if active is not None:
        stmt = stmt.where(Source.is_active.is_(active))
    if quarantined is True:
        stmt = stmt.where(Source.quarantined_at.is_not(None))
    elif quarantined is False:
        stmt = stmt.where(Source.quarantined_at.is_(None))
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.post("/sources", response_model=SourceOut, status_code=201)
async def create_source(
    payload: SourceCreate, session: SessionDep, auth: RequireWrite
) -> Source:
    existing = await session.execute(select(Source).where(Source.system_url == str(payload.system_url)))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Quelle mit dieser URL existiert bereits")

    source = Source(
        name=payload.name,
        system_url=str(payload.system_url),
        is_active=payload.is_active,
        sync_interval_min=payload.sync_interval_min,
        config=payload.config,
    )
    session.add(source)
    await session.flush()
    await session.refresh(source)
    await write_audit_log(
        session, auth, "source.create", "source", str(source.id),
        details={"name": source.name, "system_url": source.system_url},
    )
    return source


@router.get("/sources/{source_id}", response_model=SourceOut)
async def get_source(source_id: UUID, session: SessionDep, auth: RequireRead) -> Source:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    return source


@router.patch("/sources/{source_id}", response_model=SourceOut)
async def update_source(
    source_id: UUID, payload: SourceUpdate, session: SessionDep, auth: RequireWrite
) -> Source:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    update_data = payload.model_dump(exclude_unset=True)
    changed = {k: v for k, v in update_data.items() if getattr(source, k) != v}
    for key, value in update_data.items():
        setattr(source, key, value)
    await session.flush()
    await session.refresh(source)
    await write_audit_log(
        session, auth, "source.update", "source", str(source.id),
        details={"changed": changed},
    )
    return source


@router.delete("/sources/{source_id}", status_code=204)
async def delete_source(source_id: UUID, session: SessionDep, auth: RequireAdmin) -> None:
    """Destruktiv (CASCADE auf Bodies+Entities) — nur admin-Scope."""
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    name = source.name
    await session.delete(source)
    await write_audit_log(
        session, auth, "source.delete", "source", str(source_id),
        details={"name": name},
    )


# =============================================================================
# Source — Quarantäne + Bulk
# =============================================================================


@router.post("/sources/{source_id}/unquarantine", response_model=SourceOut)
async def unquarantine_source(
    source_id: UUID, session: SessionDep, auth: RequireWrite
) -> Source:
    """
    Setzt consecutive_failures=0, is_active=true, quarantined_at=null.
    Sinnvoll nachdem das Problem einer Quelle behoben wurde.
    """
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    source.consecutive_failures = 0
    source.quarantined_at = None
    source.is_active = True
    source.last_error = None
    await session.flush()
    await session.refresh(source)
    await write_audit_log(
        session, auth, "source.unquarantine", "source", str(source_id),
    )
    return source


@router.post("/sources/bulk-activate", response_model=BulkActionResult)
async def bulk_activate(
    payload: BulkSourceIdsRequest, session: SessionDep, auth: RequireWrite
) -> BulkActionResult:
    existing_ids = set(
        (await session.execute(select(Source.id).where(Source.id.in_(payload.source_ids)))).scalars()
    )
    not_found = [sid for sid in payload.source_ids if sid not in existing_ids]
    if existing_ids:
        await session.execute(
            update(Source).where(Source.id.in_(existing_ids)).values(is_active=True)
        )
    await write_audit_log(
        session, auth, "source.bulk_activate", "source", None,
        details={"count": len(existing_ids), "ids": [str(i) for i in existing_ids]},
    )
    return BulkActionResult(affected=len(existing_ids), not_found=not_found)


@router.post("/sources/bulk-deactivate", response_model=BulkActionResult)
async def bulk_deactivate(
    payload: BulkSourceIdsRequest, session: SessionDep, auth: RequireWrite
) -> BulkActionResult:
    existing_ids = set(
        (await session.execute(select(Source.id).where(Source.id.in_(payload.source_ids)))).scalars()
    )
    not_found = [sid for sid in payload.source_ids if sid not in existing_ids]
    if existing_ids:
        await session.execute(
            update(Source).where(Source.id.in_(existing_ids)).values(is_active=False)
        )
    await write_audit_log(
        session, auth, "source.bulk_deactivate", "source", None,
        details={"count": len(existing_ids)},
    )
    return BulkActionResult(affected=len(existing_ids), not_found=not_found)


@router.post("/sources/import", response_model=CsvImportResult)
async def import_sources_csv(
    session: SessionDep,
    auth: RequireWrite,
    file: UploadFile,
    activate: bool = Query(False, description="Importierte Quellen sofort aktivieren"),
) -> CsvImportResult:
    """
    Importiert Quellen aus einer CSV (Spalten: name, system_url).
    Idempotent: bereits existierende system_urls werden übersprungen.
    """
    try:
        content = (await file.read()).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(400, f"CSV muss UTF-8-encoded sein: {exc}") from exc

    reader = csv.DictReader(io.StringIO(content))
    inserted = 0
    skipped = 0
    invalid = 0

    existing_urls = set(
        (await session.execute(select(Source.system_url))).scalars().all()
    )

    for row in reader:
        name = (row.get("name") or "").strip()
        url = (row.get("system_url") or "").strip()
        if not name or not url or not (url.startswith("http://") or url.startswith("https://")):
            invalid += 1
            continue
        if url in existing_urls:
            skipped += 1
            continue
        session.add(Source(name=name, system_url=url, is_active=activate))
        existing_urls.add(url)
        inserted += 1

    await session.flush()
    await write_audit_log(
        session, auth, "source.import_csv", "source", None,
        details={"inserted": inserted, "skipped": skipped, "invalid": invalid, "activate": activate},
    )
    return CsvImportResult(inserted=inserted, skipped_duplicate=skipped, invalid_rows=invalid)


# =============================================================================
# Source — Stats
# =============================================================================


@router.get("/sources/{source_id}/stats", response_model=SourceStatsOut)
async def get_source_stats(
    source_id: UUID, session: SessionDep, auth: RequireRead
) -> SourceStatsOut:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")

    body_ids_subq = select(Body.id).where(Body.source_id == source_id).scalar_subquery()

    async def _count(model, filter_stmt=None) -> int:
        stmt = select(func.count()).select_from(model).where(model.body_id.in_(body_ids_subq))
        if filter_stmt is not None:
            stmt = stmt.where(filter_stmt)
        return (await session.execute(stmt)).scalar_one()

    bodies_count = (
        await session.execute(select(func.count()).select_from(Body).where(Body.source_id == source_id))
    ).scalar_one()

    orgs = await _count(Organization)
    persons = await _count(Person)
    meetings = await _count(Meeting)
    papers = await _count(Paper)
    files_total = await _count(File)
    files_with_text = await _count(File, File.text_content.is_not(None))
    files_pending = await _count(File, File.text_extraction_status == "pending")
    files_failed = await _count(File, File.text_extraction_status == "failed")

    # Letzter SyncLog
    last_log = (
        await session.execute(
            select(SyncLog)
            .where(SyncLog.source_id == source_id)
            .order_by(SyncLog.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    return SourceStatsOut(
        source_id=source.id,
        name=source.name,
        is_active=source.is_active,
        bodies=bodies_count,
        organizations=orgs,
        persons=persons,
        meetings=meetings,
        papers=papers,
        files=files_total,
        files_with_text=files_with_text,
        files_pending=files_pending,
        files_failed=files_failed,
        last_sync_at=source.last_sync_at,
        last_success_at=source.last_success_at,
        last_sync_duration_seconds=last_log.duration_seconds if last_log else None,
        consecutive_failures=source.consecutive_failures,
        quarantined_at=source.quarantined_at,
    )


# =============================================================================
# Sync
# =============================================================================


@router.post("/sources/{source_id}/sync", response_model=SyncResponse)
async def trigger_sync(
    source_id: UUID, payload: SyncRequest, session: SessionDep, auth: RequireWrite
) -> SyncResponse:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")

    # Inline starten (unser Setup nutzt kein Prefect-Deployment)
    from ingestor.flows.sync_source import sync_source_flow

    asyncio.create_task(sync_source_flow(source_id, full=payload.full))
    await write_audit_log(
        session, auth, "source.sync_trigger", "source", str(source_id),
        details={"full": payload.full},
    )
    return SyncResponse(
        accepted=True,
        flow_run_id=None,
        message=f"Sync gestartet (full={payload.full})",
    )


# =============================================================================
# Files / OCR
# =============================================================================


@router.post("/files/{file_id}/retry-ocr", response_model=FileOcrRetryResult)
async def retry_ocr(
    file_id: UUID, session: SessionDep, auth: RequireWrite
) -> FileOcrRetryResult:
    """Setzt die Datei wieder auf pending — der OCR-Worker picked sie im nächsten Zyklus auf."""
    file = (await session.execute(select(File).where(File.id == file_id))).scalar_one_or_none()
    if not file:
        raise HTTPException(404, "File nicht gefunden")
    previous = file.text_extraction_status
    file.text_extraction_status = "pending"
    file.text_extraction_error = None
    await session.flush()
    await write_audit_log(
        session, auth, "file.retry_ocr", "file", str(file_id),
        details={"previous_status": previous},
    )
    return FileOcrRetryResult(
        file_id=file_id, previous_status=previous, new_status="pending"
    )


@router.post("/files/retry-failed-ocr", response_model=BulkOcrRetryResult)
async def retry_all_failed_ocr(session: SessionDep, auth: RequireWrite) -> BulkOcrRetryResult:
    result = await session.execute(
        update(File)
        .where(File.text_extraction_status == "failed")
        .values(text_extraction_status="pending", text_extraction_error=None)
    )
    await write_audit_log(
        session, auth, "file.retry_all_failed_ocr", "file", None,
        details={"reset_count": result.rowcount},
    )
    return BulkOcrRetryResult(reset_count=result.rowcount or 0)


# =============================================================================
# Enrichment
# =============================================================================


@router.post("/bodies/{body_id}/enrich", response_model=EnrichmentResult)
async def trigger_enrichment(
    body_id: UUID, session: SessionDep, auth: RequireWrite
) -> EnrichmentResult:
    """Triggert Personenbilder + Geocoding für einen Body. Synchron (kann 10-60s dauern)."""
    body = (await session.execute(select(Body).where(Body.id == body_id))).scalar_one_or_none()
    if not body:
        raise HTTPException(404, "Body nicht gefunden")

    from ingestor.config import get_settings
    from ingestor.flows.tasks.person_photos import fetch_person_photos

    settings = get_settings()
    result = EnrichmentResult(body_id=body_id)

    try:
        photos = await fetch_person_photos(body_id)
        result.photos_tried = photos.get("tried", 0)
        result.photos_success = photos.get("success", 0)
    except Exception as exc:
        logger.warning("Photo enrichment failed: %s", exc)

    # Hinweis: Geocoding ist seit v0.2 in einem dedizierten Worker
    # (workers/geocoding.py) ausgelagert, der strikt seriell und mit
    # GEOCODING_INTERVAL_SECONDS (default 5s) zwischen Requests arbeitet —
    # konform zur Nominatim-Usage-Policy. Hier kein synchroner Trigger mehr.
    # Der Worker pickt neue Locations ohne lat/lon binnen Sekunden ab.

    await write_audit_log(
        session, auth, "body.enrich", "body", str(body_id),
        details=result.model_dump(mode="json"),
    )
    return result


# =============================================================================
# Sync-Logs
# =============================================================================


@router.get("/logs", response_model=list[SyncLogOut])
async def list_logs(
    session: SessionDep,
    auth: RequireRead,
    source_id: UUID | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[SyncLog]:
    stmt = select(SyncLog).order_by(SyncLog.started_at.desc()).limit(limit)
    if source_id:
        stmt = stmt.where(SyncLog.source_id == source_id)
    if status:
        stmt = stmt.where(SyncLog.status == status)
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.get("/logs/{log_id}", response_model=SyncLogOut)
async def get_log(log_id: int, session: SessionDep, auth: RequireRead) -> SyncLog:
    log = (await session.execute(select(SyncLog).where(SyncLog.id == log_id))).scalar_one_or_none()
    if not log:
        raise HTTPException(404, "Log nicht gefunden")
    return log


# =============================================================================
# Stats (global)
# =============================================================================


@router.get("/stats", response_model=StatsOut)
async def get_stats(session: SessionDep, auth: RequireRead) -> StatsOut:
    async def count(model) -> int:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()

    async def count_where(model, condition) -> int:
        return (
            await session.execute(select(func.count()).select_from(model).where(condition))
        ).scalar_one()

    return StatsOut(
        sources_total=await count(Source),
        sources_active=await count_where(Source, Source.is_active.is_(True)),
        sources_quarantined=await count_where(Source, Source.quarantined_at.is_not(None)),
        bodies=await count(Body),
        organizations=await count(Organization),
        persons=await count(Person),
        meetings=await count(Meeting),
        papers=await count(Paper),
        files=await count(File),
        files_with_text=await count_where(File, File.text_content.is_not(None)),
        files_pending=await count_where(File, File.text_extraction_status == "pending"),
        files_failed=await count_where(File, File.text_extraction_status == "failed"),
    )


# =============================================================================
# API-Keys (nur admin)
# =============================================================================


@router.get("/api-keys", response_model=list[ApiKeyOut])
async def list_api_keys(session: SessionDep, auth: RequireAdmin) -> list[ApiKeyOut]:
    keys = (await session.execute(select(ApiKey).order_by(ApiKey.created_at.desc()))).scalars().all()
    return [ApiKeyOut.from_model(k) for k in keys]


@router.post("/api-keys", response_model=ApiKeyCreateResponse, status_code=201)
async def create_api_key(
    payload: ApiKeyCreate, session: SessionDep, auth: RequireAdmin
) -> ApiKeyCreateResponse:
    """
    Erzeugt einen neuen Key. Der Klartext wird NUR einmal zurückgegeben —
    danach ist nur noch der bcrypt-Hash in der DB.
    """
    plaintext = generate_api_key()
    key = ApiKey(
        name=payload.name,
        key_hash=hash_api_key(plaintext),
        prefix=key_prefix(plaintext),
        scopes=",".join(payload.scopes),
        created_by=auth.actor,
        expires_at=payload.expires_at,
    )
    session.add(key)
    try:
        await session.flush()
    except IntegrityError as exc:
        raise HTTPException(409, "Konflikt beim Anlegen des Keys") from exc
    await session.refresh(key)
    await write_audit_log(
        session, auth, "api_key.create", "api_key", str(key.id),
        details={"name": key.name, "scopes": payload.scopes},
    )
    return ApiKeyCreateResponse(key=ApiKeyOut.from_model(key), plaintext_key=plaintext)


@router.delete("/api-keys/{key_id}", status_code=204)
async def revoke_api_key(
    key_id: UUID, session: SessionDep, auth: RequireAdmin
) -> None:
    """Soft-Delete: setzt revoked_at, Key ist ab sofort ungültig."""
    key = (await session.execute(select(ApiKey).where(ApiKey.id == key_id))).scalar_one_or_none()
    if not key:
        raise HTTPException(404, "Key nicht gefunden")
    if key.id == UUID(auth.api_key_id) if auth.api_key_id else False:
        raise HTTPException(400, "Den eigenen Key kann man nicht widerrufen")
    key.revoked_at = datetime.now(UTC)
    await write_audit_log(
        session, auth, "api_key.revoke", "api_key", str(key_id),
        details={"name": key.name},
    )


# =============================================================================
# Audit-Logs (nur admin)
# =============================================================================


@router.get("/audit-logs", response_model=list[AuditLogOut])
async def list_audit_logs(
    session: SessionDep,
    auth: RequireAdmin,
    actor: str | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[AuditLog]:
    stmt = select(AuditLog).order_by(AuditLog.timestamp.desc()).limit(limit)
    if actor:
        stmt = stmt.where(AuditLog.actor == actor)
    if action:
        stmt = stmt.where(AuditLog.action == action)
    if resource_type:
        stmt = stmt.where(AuditLog.resource_type == resource_type)
    return list((await session.execute(stmt)).scalars().all())


# =============================================================================
# Auth-Info
# =============================================================================


@router.get("/whoami")
async def whoami(auth: RequireRead) -> dict:
    """Gibt die eigene Auth-Info zurück. Nützlich für UI-Tools."""
    return {
        "actor": auth.actor,
        "api_key_id": auth.api_key_id,
        "scopes": sorted(auth.scopes),
    }
