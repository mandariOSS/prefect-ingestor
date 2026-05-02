"""
Prefect-Flow: Sync einer einzelnen OParl-Quelle.

Ausfallsicherheit:
- Jeder Body wird in einem eigenen try/except gesynct — ein fehlerhafter Body
  bricht nicht den ganzen Source-Sync ab.
- Jeder Endpoint innerhalb eines Bodies ebenfalls isoliert (asyncio.gather mit
  return_exceptions=True).
- Ein Timeout begrenzt den Gesamt-Sync pro Quelle.
- Wiederholte Fehlschläge erhöhen `consecutive_failures`; ab Schwellwert wird
  die Quelle automatisch quarantiniert (is_active=False).

Performance:
- Bodies werden innerhalb eines Sources parallel verarbeitet (Semaphore).
- Sources werden untereinander ebenfalls parallel gesynct.
- Enrichment (Personenbilder, Geocoding) wird optional automatisch getriggert.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from prefect import flow, get_run_logger
from prefect.exceptions import MissingContextError
from sqlalchemy import select

from ingestor.adapters import BaseAdapter, EntityType, get_adapter_class
from ingestor.config import get_settings
from ingestor.db import get_session
from ingestor.db.models import Source, SyncLog
from ingestor.flows.tasks.extract_embedded import extract_embedded_objects
from ingestor.flows.tasks.extract_files import extract_files_from_papers
from ingestor.flows.tasks.upsert import upsert_entity

logger = logging.getLogger(__name__)


def _logger() -> logging.Logger | logging.LoggerAdapter:
    """get_run_logger() wenn innerhalb Prefect-Context, sonst Standard-Logger."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logger

# Pro Body fetchen wir diese Entity-Typen (Reihenfolge: erst Stammdaten,
# dann Beziehungen). Wird über die Adapter-Abstraktion geladen — der Adapter
# entscheidet, ob er die Daten via OParl-HTTP oder via HTML-Scraping holt.
BODY_ENTITY_TYPES: list[EntityType] = [
    EntityType.LEGISLATIVE_TERM,
    EntityType.ORGANIZATION,
    EntityType.PERSON,
    EntityType.MEMBERSHIP,
    EntityType.MEETING,
    EntityType.PAPER,
    EntityType.AGENDA_ITEM,
    EntityType.CONSULTATION,
    EntityType.FILE,
    EntityType.LOCATION,
]


async def fetch_source_bodies(adapter: BaseAdapter) -> list[dict]:
    """Lädt alle Bodies einer Quelle über den Adapter."""
    log = _logger()
    log.info(
        "Discovering bodies via %s for %s",
        adapter.ADAPTER_TYPE,
        adapter.source.system_url,
    )
    bodies = [b async for b in adapter.discover_bodies()]
    log.info("Found %d bodies", len(bodies))
    return bodies


async def sync_body_endpoint(
    adapter: BaseAdapter,
    body: dict,
    body_uuid: UUID,
    entity_type: EntityType,
    modified_since: datetime | None,
) -> int:
    """Synchronisiert eine einzelne Entity-Liste eines Bodies.

    Returns: Anzahl synchronisierter Entitäten.

    Rezeptiv für ``NotImplementedError`` aus Skeleton-Adaptern: wir behandeln
    sie als 0 Items (kein Fehler, weil "Adapter unterstützt diesen Typ noch
    nicht" eine valide Übergangs-Situation ist während wir Skelette ausbauen).
    """
    count = 0
    try:
        async for item in adapter.list_entities(
            body, entity_type, modified_since=modified_since
        ):
            try:
                await upsert_entity(item, body_id=body_uuid)
                count += 1
            except Exception as exc:
                logger.warning("Upsert failed for %s: %s", item.get("id"), exc)
    except NotImplementedError as exc:
        # Skeleton-Adapter — kein Fehler, nur Hinweis im Log
        logger.debug(
            "%s: %s nicht implementiert — skip (%s)",
            adapter.ADAPTER_TYPE,
            entity_type.value,
            exc,
        )
    return count


async def _sync_single_body(
    adapter: BaseAdapter,
    body_json: dict,
    body_uuid: UUID,
    modified_since: datetime | None,
) -> tuple[dict[str, int], list[str]]:
    """Synct einen einzelnen Body über den Adapter.

    Isoliert: Fehler pro Entity-Typ/Enrichment werden gesammelt, killen
    aber nicht den gesamten Sync.
    """
    short = body_json.get("shortName", "?")
    counts: dict[str, int] = {}
    errors: list[str] = []

    # Alle Entity-Typen parallel laden
    tasks = [
        sync_body_endpoint(adapter, body_json, body_uuid, et, modified_since=modified_since)
        for et in BODY_ENTITY_TYPES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for et, res in zip(BODY_ENTITY_TYPES, results, strict=False):
        if isinstance(res, Exception):
            errors.append(f"{short}.{et.value}: {res}")
        elif isinstance(res, int):
            counts[et.value] = counts.get(et.value, 0) + res

    # Embedded-Objects (OParl 1.0 Kompatibilität)
    try:
        embedded = await extract_embedded_objects(body_uuid)
        for key, val in embedded.items():
            counts[f"embedded_{key}"] = counts.get(f"embedded_{key}", 0) + val
    except Exception as exc:
        errors.append(f"{short}.embedded: {exc}")

    # File-URLs aus Paper-JSON
    try:
        file_count = await extract_files_from_papers(body_uuid)
        counts["files_extracted"] = counts.get("files_extracted", 0) + file_count
    except Exception as exc:
        errors.append(f"{short}.files_extract: {exc}")

    return counts, errors


async def _run_enrichment(body_uuid: UUID, short: str) -> dict[str, int]:
    """Triggert optional Personenbilder + Geocoding nach Body-Sync."""
    settings = get_settings()
    out: dict[str, int] = {}

    if settings.enrichment_auto_photos:
        try:
            from ingestor.flows.tasks.person_photos import fetch_person_photos

            res = await fetch_person_photos(body_uuid)
            out["photos_tried"] = res.get("tried", 0)
            out["photos_success"] = res.get("success", 0)
        except Exception as exc:
            logger.warning("Photo enrichment failed for %s: %s", short, exc)

    # Geocoding wird NICHT mehr im Sync-Flow gemacht — der dedizierte
    # GeocodingWorker (siehe workers/geocoding.py) übernimmt das seriell
    # und Rate-Limit-konform. Sync schreibt nur die Adress-Spalten in
    # locations, der Worker pickt sie binnen Sekunden ab.
    # Das alte enrichment_auto_geocoding-Setting wird ignoriert.

    return out


async def _finalize_sync(
    source_id: UUID,
    log_id: int,
    status: str,
    counts: dict,
    errors: list[str],
    started: datetime,
    full: bool,
) -> None:
    """Schreibt SyncLog + Source-Status + Quarantäne-Logik in einer Transaction."""
    settings = get_settings()
    finished = datetime.now(UTC)
    duration = (finished - started).total_seconds()

    async with get_session() as session:
        sync_log = (await session.execute(select(SyncLog).where(SyncLog.id == log_id))).scalar_one()
        sync_log.status = status
        sync_log.finished_at = finished
        sync_log.duration_seconds = duration
        sync_log.entities_synced = counts
        sync_log.errors = errors[:50]

        source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one()
        source.last_sync_at = finished
        if status == "success":
            source.last_success_at = finished
            source.last_error = None
            source.consecutive_failures = 0
        else:
            source.last_error = "; ".join(errors[:3]) if errors else status
            if status == "failed":
                source.consecutive_failures = (source.consecutive_failures or 0) + 1
                if source.consecutive_failures >= settings.sync_quarantine_threshold:
                    source.is_active = False
                    source.quarantined_at = finished
                    logger.error(
                        "Source %s quarantined after %d consecutive failures",
                        source.name,
                        source.consecutive_failures,
                    )
            # "partial" (mindestens ein Body erfolgreich) setzt den Counter nicht hoch,
            # reset ihn aber auch nicht — konservative Behandlung
        if full:
            source.last_full_sync_at = finished


@flow(name="sync-source", log_prints=True)
async def sync_source_flow(source_id: UUID, full: bool = False) -> dict:
    """
    Haupt-Flow für eine Quelle. Fehlerisoliert — ein fehlerhafter Body
    blockiert nicht die anderen.
    """
    log = _logger()
    settings = get_settings()
    started = datetime.now(UTC)

    async with get_session() as session:
        source = (await session.execute(select(Source).where(Source.id == source_id))).scalar_one_or_none()
        if not source:
            raise ValueError(f"Source {source_id} not found")
        modified_since = None if full else source.last_sync_at
        source_name = source.name
        # Adapter-Type aus DB lesen (default: oparl). Sync-Flow kennt keine
        # Vendor-Specials — der Adapter abstrahiert OParl-HTTP vs. RIS-HTML.
        adapter_cls = get_adapter_class(source.adapter_type)
        # Wir reichen die ganze Source-Instanz an den Adapter weiter, damit
        # Scraper auch ``source.config`` lesen können. Detached-Object: wir
        # reload-en sie im Adapter-Scope unten neu, damit kein Session-Leak
        # entsteht.
        source_obj = source

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
    status = "failed"

    try:
        async with asyncio.timeout(settings.sync_source_timeout_seconds):
            async with adapter_cls(source_obj) as adapter:
                try:
                    bodies = await fetch_source_bodies(adapter)
                except NotImplementedError as exc:
                    errors.append(
                        f"{source_name}.discover_bodies: adapter '{adapter.ADAPTER_TYPE}'"
                        f" is a SKELETON ({exc})"
                    )
                    log.warning(
                        "Adapter %s is skeleton — no bodies discovered",
                        adapter.ADAPTER_TYPE,
                    )
                    bodies = []
                except Exception as exc:
                    errors.append(f"{source_name}.fetch_bodies: {exc}")
                    log.exception("Failed to fetch bodies")
                    bodies = []

                # Bodies upserten
                body_uuid_map: dict[str, UUID] = {}
                for body_json in bodies:
                    try:
                        body_uuid = await upsert_entity(body_json, extra_fields={"source_id": source_id})
                        if body_uuid:
                            body_uuid_map[body_json["id"]] = body_uuid
                    except Exception as exc:
                        errors.append(f"{source_name}.body_upsert({body_json.get('id')}): {exc}")
                counts["bodies"] = len(body_uuid_map)

                # Body-Syncs parallel (Semaphore begrenzt Last)
                sem = asyncio.Semaphore(settings.sync_body_concurrency)

                async def sync_body_safe(bj: dict) -> tuple[dict[str, int], list[str]]:
                    async with sem:
                        uuid_ = body_uuid_map.get(bj["id"])
                        if not uuid_:
                            return {}, []
                        try:
                            body_counts, body_errors = await _sync_single_body(
                                adapter, bj, uuid_, modified_since
                            )
                            enrich = await _run_enrichment(uuid_, bj.get("shortName", "?"))
                            body_counts.update(enrich)
                            return body_counts, body_errors
                        except Exception as exc:
                            log.exception("Body sync crashed for %s", bj.get("id"))
                            return {}, [f"{bj.get('shortName', '?')}: {exc}"]

                body_results = await asyncio.gather(*[sync_body_safe(b) for b in bodies])
                for body_counts, body_errors in body_results:
                    for k, v in body_counts.items():
                        counts[k] = counts.get(k, 0) + v
                    errors.extend(body_errors)

        # Status bestimmen
        if not bodies:
            status = "failed"
        elif errors and any("fetch_bodies" in e or "body_upsert" in e for e in errors):
            status = "failed" if counts.get("bodies", 0) == 0 else "partial"
        elif errors:
            status = "partial"
        else:
            status = "success"

    except TimeoutError:
        errors.append(f"Sync-Timeout nach {settings.sync_source_timeout_seconds}s")
        status = "failed"
        log.error("Sync timed out for %s", source_name)
    except Exception as exc:
        log.exception("Sync crashed: %s", exc)
        errors.append(str(exc))
        status = "failed"

    try:
        await _finalize_sync(source_id, log_id, status, counts, errors, started, full)
    except Exception:
        log.exception("Failed to finalize sync — metadata may be inconsistent")

    duration = (datetime.now(UTC) - started).total_seconds()
    log.info("Sync complete: %s in %.1fs — %s", status, duration, counts)
    return {
        "status": status,
        "duration_seconds": duration,
        "entities_synced": counts,
        "errors": errors,
    }


@flow(name="sync-all-active-sources", log_prints=True)
async def sync_all_active_sources(full: bool = False, max_concurrent: int = 5) -> list[dict]:
    """
    Triggert sync_source_flow parallel für alle aktiven Quellen.

    Nicht quarantinierte (is_active=True) Sources werden gesynct. Jede Source
    läuft isoliert — eine kaputte Source kann die anderen nicht brechen.
    """
    log = _logger()
    async with get_session() as session:
        result = await session.execute(select(Source).where(Source.is_active.is_(True)))
        sources = list(result.scalars().all())

    log.info("Syncing %d active sources (full=%s)", len(sources), full)

    sem = asyncio.Semaphore(max_concurrent)

    async def run_one(src: Source) -> dict:
        async with sem:
            try:
                return await sync_source_flow(src.id, full=full)
            except Exception as exc:
                log.exception("Source %s failed catastrophically", src.name)
                return {"status": "failed", "source": src.name, "error": str(exc)}

    results = await asyncio.gather(*[run_one(s) for s in sources])
    successful = sum(1 for r in results if r.get("status") == "success")
    log.info("Sync-all done: %d/%d success", successful, len(sources))
    return results
