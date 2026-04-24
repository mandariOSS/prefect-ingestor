"""
Tests für den generischen Upsert-Task.

Verifiziert:
- Neue Entitäten werden angelegt
- Bestehende Entitäten werden aktualisiert
- Enrichment-Felder (text_content, photo_data, latitude, …) werden NICHT
  vom Sync überschrieben
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select

from ingestor.db import async_session_factory
from ingestor.db.models import Body, File, Paper, Source
from ingestor.flows.tasks.upsert import upsert_entity

pytestmark = [pytest.mark.asyncio, pytest.mark.db]


async def _make_source_and_body():
    async with async_session_factory() as s:
        src = Source(
            name="Test Stadt",
            system_url=f"https://test-{uuid4()}.example/system",
            is_active=True,
        )
        s.add(src)
        await s.flush()

        body = Body(
            source_id=src.id,
            external_id=f"https://test.example/body/{uuid4()}",
            name="Body 1",
            raw={"id": "x", "type": "https://schema.oparl.org/1.1/Body"},
        )
        s.add(body)
        await s.flush()
        await s.commit()
        return src.id, body.id, body.external_id


async def test_upsert_inserts_new_paper(db_session):
    _, body_id, _ = await _make_source_and_body()

    paper_data = {
        "id": "https://test.example/paper/42",
        "type": "https://schema.oparl.org/1.1/Paper",
        "name": "Testvorlage",
        "reference": "V/42/2024",
        "paperType": "Antrag",
    }

    result_id = await upsert_entity(paper_data, body_id=body_id)
    assert result_id is not None

    async with async_session_factory() as s:
        paper = (
            await s.execute(select(Paper).where(Paper.external_id == paper_data["id"]))
        ).scalar_one()

    assert paper.name == "Testvorlage"
    assert paper.reference == "V/42/2024"
    assert paper.body_id == body_id


async def test_upsert_preserves_enrichment_fields(db_session):
    """OCR-befüllte Felder dürfen beim Sync-Update NICHT überschrieben werden."""
    _, body_id, _ = await _make_source_and_body()

    # File mit OCR-Daten anlegen
    async with async_session_factory() as s:
        file = File(
            body_id=body_id,
            external_id="https://test.example/file/1",
            name="Dokument.pdf",
            raw={
                "id": "https://test.example/file/1",
                "type": "https://schema.oparl.org/1.1/File",
                "name": "Dokument.pdf",
            },
            text_content="OCR-Text aus Worker",
            text_extraction_status="done",
            text_extraction_method="pypdf",
        )
        s.add(file)
        await s.commit()

    # Sync-Update (name geändert)
    new_data = {
        "id": "https://test.example/file/1",
        "type": "https://schema.oparl.org/1.1/File",
        "name": "Dokument-v2.pdf",
        "fileName": "dokument_v2.pdf",
    }
    await upsert_entity(new_data, body_id=body_id)

    async with async_session_factory() as s:
        file_after = (
            await s.execute(select(File).where(File.external_id == new_data["id"]))
        ).scalar_one()

    assert file_after.name == "Dokument-v2.pdf"  # Sync-Feld aktualisiert
    assert file_after.text_content == "OCR-Text aus Worker"  # OCR-Feld unverändert
    assert file_after.text_extraction_status == "done"
    assert file_after.text_extraction_method == "pypdf"
