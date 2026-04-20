"""
Task: Eingebettete OParl-Objekte aus übergeordneten Entitäten extrahieren.

OParl 1.0 Bodies haben KEINE separaten Endpoints für Memberships,
AgendaItems, Consultations, Files. Stattdessen sind diese Objekte
INLINE in den übergeordneten Entitäten eingebettet:

- Person.membership[] → Memberships (inline)
- Meeting.agendaItem[] → AgendaItems (inline)
- Meeting.auxiliaryFile[] → Files (inline)
- Paper.consultation[] → Consultations (inline)
- Paper.mainFile / Paper.auxiliaryFile[] → Files (inline)

Dieser Task liest die gespeicherten raw-JSON-Daten und erstellt
separate DB-Einträge für die eingebetteten Objekte.
"""

from __future__ import annotations

import logging
from uuid import UUID

from prefect import task
from sqlalchemy import select

from ingestor.db import get_session
from ingestor.db.models import Location, Meeting, Paper, Person
from ingestor.flows.tasks.upsert import upsert_entity

logger = logging.getLogger(__name__)


@task(name="extract-embedded-objects", retries=1)
async def extract_embedded_objects(body_id: UUID) -> dict:
    """
    Extrahiert eingebettete Objekte aus raw-JSON für einen Body.

    Nötig für OParl 1.0 Quellen, die keine separaten Endpoints haben.
    Bei 1.1 Quellen sind die Daten schon separat — Duplikate werden
    durch upsert via external_id verhindert.

    Returns: {"memberships": int, "agenda_items": int, "consultations": int, "files": int, "locations": int}
    """
    counts = {"memberships": 0, "agenda_items": 0, "consultations": 0, "files": 0, "locations": 0}

    # 1. Memberships aus Person.membership[]
    async with get_session() as session:
        persons = (
            (
                await session.execute(
                    select(Person).where(Person.body_id == body_id, Person.deleted.is_(False))
                )
            )
            .scalars()
            .all()
        )

    for person in persons:
        raw = person.raw or {}
        memberships = raw.get("membership") or []
        for m_data in memberships:
            if isinstance(m_data, dict) and m_data.get("id"):
                try:
                    result = await upsert_entity(m_data, body_id=body_id)
                    if result:
                        counts["memberships"] += 1
                except Exception as exc:
                    logger.debug("Membership upsert skipped: %s", exc)

    # 2. AgendaItems + Files aus Meeting
    async with get_session() as session:
        meetings = (
            (
                await session.execute(
                    select(Meeting).where(Meeting.body_id == body_id, Meeting.deleted.is_(False))
                )
            )
            .scalars()
            .all()
        )

    for meeting in meetings:
        raw = meeting.raw or {}

        # AgendaItems
        agenda_items = raw.get("agendaItem") or []
        for ai_data in agenda_items:
            if isinstance(ai_data, dict) and ai_data.get("id"):
                try:
                    result = await upsert_entity(ai_data, body_id=body_id)
                    if result:
                        counts["agenda_items"] += 1
                except Exception:
                    pass

        # Files (invitation, resultsProtocol, verbatimProtocol, auxiliaryFile)
        for file_field in ["invitation", "resultsProtocol", "verbatimProtocol"]:
            file_data = raw.get(file_field)
            if isinstance(file_data, dict) and file_data.get("id"):
                try:
                    result = await upsert_entity(file_data, body_id=body_id)
                    if result:
                        counts["files"] += 1
                except Exception:
                    pass

        aux_files = raw.get("auxiliaryFile") or []
        for f_data in aux_files:
            if isinstance(f_data, dict) and f_data.get("id"):
                result = await upsert_entity(f_data, body_id=body_id)
                if result:
                    counts["files"] += 1

    # 3. Consultations + Files aus Paper
    async with get_session() as session:
        papers = (
            (await session.execute(select(Paper).where(Paper.body_id == body_id, Paper.deleted.is_(False))))
            .scalars()
            .all()
        )

    for paper in papers:
        raw = paper.raw or {}

        # Consultations
        consultations = raw.get("consultation") or []
        for c_data in consultations:
            if isinstance(c_data, dict) and c_data.get("id"):
                result = await upsert_entity(c_data, body_id=body_id)
                if result:
                    counts["consultations"] += 1

        # mainFile
        main_file = raw.get("mainFile")
        if isinstance(main_file, dict) and main_file.get("id"):
            result = await upsert_entity(main_file, body_id=body_id)
            if result:
                counts["files"] += 1

        # auxiliaryFile
        aux_files = raw.get("auxiliaryFile") or []
        for f_data in aux_files:
            if isinstance(f_data, dict) and f_data.get("id"):
                result = await upsert_entity(f_data, body_id=body_id)
                if result:
                    counts["files"] += 1

    # 4. Locations aus Meeting.location, Person.locationObject, Body.location, Organization.location
    # Meetings haben oft ein inline location-Objekt mit Adresse
    for meeting in meetings:
        raw = meeting.raw or {}
        loc_data = raw.get("location")
        if isinstance(loc_data, dict) and loc_data.get("id"):
            result = await upsert_entity(loc_data, body_id=body_id)
            if result:
                counts["locations"] += 1
        elif isinstance(loc_data, dict) and loc_data.get("description"):
            # Location ohne eigene id — erstelle synthetische
            await _upsert_inline_location(loc_data, body_id, f"meeting:{meeting.external_id}")
            counts["locations"] += 1

    # Person.locationObject (OParl 1.1)
    for person in persons:
        raw = person.raw or {}
        loc_data = raw.get("locationObject") or raw.get("location")
        if isinstance(loc_data, dict):
            if loc_data.get("id"):
                result = await upsert_entity(loc_data, body_id=body_id)
                if result:
                    counts["locations"] += 1

    # Paper.location[] (Array von inline-Locations)
    for paper in papers:
        raw = paper.raw or {}
        paper_locations = raw.get("location") or []
        if isinstance(paper_locations, list):
            for loc_data in paper_locations:
                if isinstance(loc_data, dict) and loc_data.get("id"):
                    result = await upsert_entity(loc_data, body_id=body_id)
                    if result:
                        counts["locations"] += 1
                elif isinstance(loc_data, dict) and loc_data.get("description"):
                    await _upsert_inline_location(loc_data, body_id, f"paper:{paper.external_id}")
                    counts["locations"] += 1

    logger.info(
        "Extracted embedded objects for body %s: %s",
        body_id,
        counts,
    )
    return counts


async def _upsert_inline_location(loc_data: dict, body_id: UUID, context_id: str) -> None:
    """
    Erstellt einen Location-Eintrag aus einem inline-Location ohne eigene id.

    Viele OParl-Server liefern Locations inline (ohne id-URL) mit nur
    description/streetAddress. Wir erstellen einen synthetischen Eintrag.
    """
    from uuid import uuid4

    from sqlalchemy import select

    description = loc_data.get("description", "")
    street = loc_data.get("streetAddress", "")
    synthetic_id = f"inline:{context_id}:{description}:{street}"

    async with get_session() as session:
        # Prüfe ob schon existiert
        existing = (
            await session.execute(
                select(Location).where(Location.external_id == synthetic_id, Location.body_id == body_id)
            )
        ).scalar_one_or_none()

        if existing:
            return

        loc = Location(
            id=uuid4(),
            body_id=body_id,
            external_id=synthetic_id,
            description=description,
            street_address=street,
            locality=loc_data.get("locality"),
            postal_code=loc_data.get("postalCode"),
            raw=loc_data,
        )

        # GeoJSON extrahieren falls vorhanden
        geojson = loc_data.get("geojson")
        if isinstance(geojson, dict):
            geometry = geojson.get("geometry") or {}
            coords = geometry.get("coordinates") or []
            if len(coords) >= 2:
                loc.longitude = coords[0]
                loc.latitude = coords[1]

        session.add(loc)
