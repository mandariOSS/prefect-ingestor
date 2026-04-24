"""
Task: Files aus Paper-JSON extrahieren.

OParl-Papers enthalten File-URLs in `mainFile` und `auxiliaryFile`.
Bei OParl 1.0 sind Files oft inline im Paper, bei 1.1 als separate URLs.
Dieser Task liest die gesynchten Papers aus der DB und erstellt File-Einträge
mit den originalen Download-URLs (kein Download der Dateien selbst).
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select

from ingestor.db import get_session
from ingestor.db.models import File, Paper

logger = logging.getLogger(__name__)


async def extract_files_from_papers(body_id: UUID) -> int:
    """
    Extrahiert File-Referenzen aus gesynchten Paper-JSONB-Daten.

    Erstellt File-Einträge mit den originalen Download-URLs.
    Dateien werden NICHT heruntergeladen (spart Speicher).

    Returns: Anzahl neu erstellter Files.
    """
    created = 0

    async with get_session() as session:
        # Papers mit raw-JSON laden
        result = await session.execute(
            select(Paper).where(Paper.body_id == body_id, Paper.deleted.is_(False))
        )
        papers = result.scalars().all()

        for paper in papers:
            raw = paper.raw or {}

            # mainFile (einzelne URL oder dict)
            main_file = raw.get("mainFile")
            if main_file:
                files_data = [main_file] if isinstance(main_file, (str, dict)) else []
                for fd in files_data:
                    if await _upsert_file(session, fd, body_id, paper.id):
                        created += 1

            # auxiliaryFile (Liste von URLs oder dicts)
            aux_files = raw.get("auxiliaryFile") or []
            if isinstance(aux_files, list):
                for fd in aux_files:
                    if await _upsert_file(session, fd, body_id, paper.id):
                        created += 1

    logger.info("Extracted %d files from %d papers for body %s", created, len(papers), body_id)
    return created


async def _upsert_file(session: Any, file_data: str | dict, body_id: UUID, paper_id: UUID) -> bool:
    """Erstellt oder aktualisiert einen File-Eintrag. Gibt True zurück wenn neu erstellt."""
    if isinstance(file_data, str):
        # Nur URL — minimaler File-Eintrag
        external_id = file_data
        raw = {"id": file_data, "type": "https://schema.oparl.org/1.1/File"}
        name = None
        file_name = None
        mime_type = None
        size = None
        access_url = file_data
        download_url = file_data
    elif isinstance(file_data, dict):
        external_id = file_data.get("id")
        if not external_id:
            return False
        raw = file_data
        name = file_data.get("name")
        file_name = file_data.get("fileName")
        mime_type = file_data.get("mimeType")
        size = file_data.get("size")
        access_url = file_data.get("accessUrl")
        download_url = file_data.get("downloadUrl")
    else:
        return False

    # Check ob schon existiert
    existing = (
        await session.execute(select(File).where(File.external_id == external_id, File.body_id == body_id))
    ).scalar_one_or_none()

    if existing:
        return False  # Schon vorhanden

    file_obj = File(
        id=uuid4(),
        body_id=body_id,
        paper_id=paper_id,
        external_id=external_id,
        name=name,
        file_name=file_name,
        mime_type=mime_type,
        size=size,
        access_url=access_url,
        download_url=download_url,
        raw=raw,
        text_extraction_status="pending" if mime_type and "pdf" in (mime_type or "").lower() else "skipped",
    )
    session.add(file_obj)
    return True
