"""Management-API: Sources verwalten, Jobs triggern, Logs ansehen, Statistiken."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select

from ingestor.api.dependencies import SessionDep, require_api_key
from ingestor.api.management.schemas import (
    SourceCreate,
    SourceOut,
    SourceUpdate,
    StatsOut,
    SyncLogOut,
    SyncRequest,
    SyncResponse,
)
from ingestor.db.models import (
    Body,
    File,
    Meeting,
    Organization,
    Paper,
    Person,
    Source,
    SyncLog,
)

router = APIRouter(
    tags=["management"],
    dependencies=[Depends(require_api_key)],
)


# =============================================================================
# Sources
# =============================================================================


@router.get("/sources", response_model=list[SourceOut])
async def list_sources(session: SessionDep) -> list[Source]:
    result = await session.execute(select(Source).order_by(Source.name))
    return list(result.scalars().all())


@router.post("/sources", response_model=SourceOut, status_code=201)
async def create_source(payload: SourceCreate, session: SessionDep) -> Source:
    existing = await session.execute(select(Source).where(Source.system_url == str(payload.system_url)))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Quelle mit dieser system_url existiert bereits")

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
    return source


@router.get("/sources/{source_id}", response_model=SourceOut)
async def get_source(source_id: UUID, session: SessionDep) -> Source:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    return source


@router.patch("/sources/{source_id}", response_model=SourceOut)
async def update_source(source_id: UUID, payload: SourceUpdate, session: SessionDep) -> Source:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    update_data = payload.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(source, key, value)
    await session.flush()
    await session.refresh(source)
    return source


@router.delete("/sources/{source_id}", status_code=204)
async def delete_source(source_id: UUID, session: SessionDep) -> None:
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")
    await session.delete(source)


@router.post("/sources/{source_id}/sync", response_model=SyncResponse)
async def trigger_sync(source_id: UUID, payload: SyncRequest, session: SessionDep) -> SyncResponse:
    """Triggert einen Sync via Prefect-Flow-Deployment."""
    source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source nicht gefunden")

    # Prefect-Flow-Run via Deployment triggern (asynchron, non-blocking)
    try:
        from prefect.deployments import run_deployment

        deployment_name = "sync-source/manual" if not payload.full else "sync-source/full"
        flow_run = await run_deployment(
            name=deployment_name,
            parameters={"source_id": str(source_id), "full": payload.full},
            timeout=0,  # don't wait for completion
        )
        return SyncResponse(
            accepted=True,
            flow_run_id=str(flow_run.id),
            message=f"Sync triggered (full={payload.full})",
        )
    except Exception as exc:  # noqa: BLE001
        # Fallback: Direkt asynchron starten ohne Prefect-Deployment
        # (für Setups ohne Prefect-Server)
        import asyncio

        from ingestor.flows.sync_source import sync_source_flow

        asyncio.create_task(sync_source_flow(source_id, full=payload.full))
        return SyncResponse(
            accepted=True,
            flow_run_id=None,
            message=f"Sync started inline (Prefect deployment unavailable: {exc})",
        )


# =============================================================================
# Sync-Logs
# =============================================================================


@router.get("/logs", response_model=list[SyncLogOut])
async def list_logs(
    session: SessionDep,
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
async def get_log(log_id: int, session: SessionDep) -> SyncLog:
    log = (await session.execute(select(SyncLog).where(SyncLog.id == log_id))).scalar_one_or_none()
    if not log:
        raise HTTPException(404, "Log nicht gefunden")
    return log


# =============================================================================
# Stats
# =============================================================================


@router.get("/stats", response_model=StatsOut)
async def get_stats(session: SessionDep) -> StatsOut:
    async def count(model) -> int:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()

    sources_total = await count(Source)
    sources_active = (
        await session.execute(select(func.count()).select_from(Source).where(Source.is_active.is_(True)))
    ).scalar_one()

    files_with_text = (
        await session.execute(
            select(func.count()).select_from(File).where(File.text_content.is_not(None))
        )
    ).scalar_one()

    return StatsOut(
        sources_total=sources_total,
        sources_active=sources_active,
        bodies=await count(Body),
        organizations=await count(Organization),
        persons=await count(Person),
        meetings=await count(Meeting),
        papers=await count(Paper),
        files=await count(File),
        files_with_text=files_with_text,
    )
