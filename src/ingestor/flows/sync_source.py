"""
Prefect-Flow: Sync einer einzelnen OParl-Quelle.

Lädt System → Bodies → für jeden Body parallel: Organizations, Persons,
Memberships, Meetings, AgendaItems, Papers, Consultations, Files, Locations.

Schreibt am Ende einen SyncLog-Eintrag.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from prefect import flow, get_run_logger, task
from sqlalchemy import select

from ingestor.db import get_session
from ingestor.db.models import Source, SyncLog
from ingestor.oparl import OParlClient

logger = logging.getLogger(__name__)

# Pro Body fetchen wir diese Endpoints (Reihenfolge: erst Stammdaten, dann Beziehungen)
BODY_ENDPOINT_FIELDS = [
    "legislativeTerm",
    "organization",
    "person",
    "membership",
    "meeting",
    "paper",
    "agendaItem",
    "consultation",
    "file",
    "location",
]


@task(name="fetch-source-bodies")
async def fetch_source_bodies(client: OParlClient, system_url: str) -> list[dict]:
    """Lädt das System und alle Bodies."""
    log = get_run_logger()
    log.info("Fetching system: %s", system_url)
    system = await client.fetch_system(system_url)
    bodies = [b async for b in client.fetch_bodies(system)]
    log.info("Found %d bodies", len(bodies))
    return bodies


@task(name="sync-body-endpoint", retries=2, retry_delay_seconds=10)
async def sync_body_endpoint(
    client: OParlClient,
    body_uuid: UUID,
    list_url: str,
    modified_since: datetime | None,
) -> int:
    """
    Synchronisiert eine einzelne Liste (z.B. Meetings eines Bodies).

    Returns: Anzahl synchronisierter Entitäten.
    """
    from ingestor.flows.tasks.upsert import upsert_entity

    count = 0
    async for item in client.list_paginated(list_url, modified_since=modified_since):
        try:
            await upsert_entity(item, body_id=body_uuid)
            count += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Upsert failed for %s: %s", item.get("id"), exc)
    return count


@flow(name="sync-source", log_prints=True)
async def sync_source_flow(source_id: UUID, full: bool = False) -> dict:
    """
    Hauptflow für eine Quelle.

    Args:
        source_id: UUID der Source in der DB
        full: True für vollständigen Sync (ignoriert modified_since)

    Returns:
        Statistik-Dict {entities_synced, errors, duration_seconds, status}
    """
    log = get_run_logger()
    started = datetime.now(timezone.utc)

    # Source aus DB laden
    async with get_session() as session:
        source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
        if not source:
            raise ValueError(f"Source {source_id} not found")
        modified_since = None if full else source.last_sync_at

    # Initialer SyncLog-Eintrag (running)
    async with get_session() as session:
        sync_log = SyncLog(
            source_id=source_id,
            sync_type="full" if full else "incremental",
            status="running",
            triggered_by="schedule",
        )
        session.add(sync_log)
        await session.flush()
        log_id = sync_log.id

    counts: dict[str, int] = {}
    errors: list[str] = []

    try:
        async with OParlClient() as client:
            bodies = await fetch_source_bodies(client, source.system_url)

            from ingestor.flows.tasks.upsert import upsert_entity

            # Alle Bodies upserten (Bodies haben source_id statt body_id)
            body_uuid_map: dict[str, UUID] = {}
            for body_json in bodies:
                body_uuid = await upsert_entity(body_json, extra_fields={"source_id": source_id})
                if body_uuid:
                    body_uuid_map[body_json["id"]] = body_uuid
            counts["bodies"] = len(body_uuid_map)

            # Für jeden Body alle Endpoints parallel
            for body_json in bodies:
                body_uuid = body_uuid_map.get(body_json["id"])
                if not body_uuid:
                    continue

                tasks = []
                for field in BODY_ENDPOINT_FIELDS:
                    list_url = body_json.get(field)
                    if not list_url:
                        continue
                    tasks.append(
                        sync_body_endpoint(client, body_uuid, list_url, modified_since=modified_since)
                    )
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for field, res in zip(BODY_ENDPOINT_FIELDS, results, strict=False):
                    if isinstance(res, Exception):
                        errors.append(f"{body_json.get('shortName', '?')}.{field}: {res}")
                    elif isinstance(res, int):
                        counts[field] = counts.get(field, 0) + res

        status = "success" if not errors else "partial"
    except Exception as exc:  # noqa: BLE001
        log.exception("Sync failed: %s", exc)
        errors.append(str(exc))
        status = "failed"

    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds()

    # Source-Status + SyncLog updaten
    async with get_session() as session:
        sync_log = (await session.execute(select(SyncLog).where(SyncLog.id == log_id))).scalar_one()
        sync_log.status = status
        sync_log.finished_at = finished
        sync_log.duration_seconds = duration
        sync_log.entities_synced = counts
        sync_log.errors = errors[:50]  # max 50 Fehler

        source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one()
        source.last_sync_at = finished
        if status == "success":
            source.last_success_at = finished
            source.last_error = None
        else:
            source.last_error = "; ".join(errors[:3])
        if full:
            source.last_full_sync_at = finished

    log.info("Sync complete: %s in %.1fs — %s", status, duration, counts)
    return {
        "status": status,
        "duration_seconds": duration,
        "entities_synced": counts,
        "errors": errors,
    }


@flow(name="sync-all-active-sources", log_prints=True)
async def sync_all_active_sources(full: bool = False) -> list[dict]:
    """Triggert sync_source_flow parallel für alle aktiven Quellen."""
    log = get_run_logger()
    async with get_session() as session:
        result = await session.execute(select(Source).where(Source.is_active.is_(True)))
        sources = result.scalars().all()

    log.info("Syncing %d active sources (full=%s)", len(sources), full)
    return await asyncio.gather(*[sync_source_flow(s.id, full=full) for s in sources])
