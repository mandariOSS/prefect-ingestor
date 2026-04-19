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
from ingestor.db.models import Meeting, Paper, Person
from ingestor.flows.tasks.upsert import upsert_entity

logger = logging.getLogger(__name__)


@task(name="extract-embedded-objects", retries=1)
async def extract_embedded_objects(body_id: UUID) -> dict:
    """
    Extrahiert eingebettete Objekte aus raw-JSON für einen Body.

    Nötig für OParl 1.0 Quellen, die keine separaten Endpoints haben.
    Bei 1.1 Quellen sind die Daten schon separat — Duplikate werden
    durch upsert via external_id verhindert.

    Returns: {"memberships": int, "agenda_items": int, "consultations": int, "files": int}
    """
    counts = {"memberships": 0, "agenda_items": 0, "consultations": 0, "files": 0}

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
                result = await upsert_entity(m_data, body_id=body_id)
                if result:
                    counts["memberships"] += 1

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
                result = await upsert_entity(ai_data, body_id=body_id)
                if result:
                    counts["agenda_items"] += 1

        # Files (invitation, resultsProtocol, verbatimProtocol, auxiliaryFile)
        for file_field in ["invitation", "resultsProtocol", "verbatimProtocol"]:
            file_data = raw.get(file_field)
            if isinstance(file_data, dict) and file_data.get("id"):
                result = await upsert_entity(file_data, body_id=body_id)
                if result:
                    counts["files"] += 1

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

    logger.info(
        "Extracted embedded objects for body %s: %s",
        body_id,
        counts,
    )
    return counts
