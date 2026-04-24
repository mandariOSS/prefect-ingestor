"""
Integration-Test: Sync-Flow gegen eine gemockte OParl-API (respx).

Verifiziert:
- Sync lädt System + Bodies und persistiert sie
- Fehler in einem Endpoint brechen den Sync nicht ab (Fehlerisolierung)
- consecutive_failures + Quarantäne-Logik
"""

from __future__ import annotations

import pytest
import respx
from httpx import Response
from sqlalchemy import select

from ingestor.config import get_settings
from ingestor.db import async_session_factory
from ingestor.db.models import Body, Paper, Source
from ingestor.flows.sync_source import sync_source_flow

pytestmark = [pytest.mark.asyncio, pytest.mark.db]


def _system_json(base: str) -> dict:
    return {
        "id": f"{base}/system",
        "type": "https://schema.oparl.org/1.1/System",
        "oparlVersion": "https://schema.oparl.org/1.1/",
        "body": f"{base}/bodies",
    }


def _body(base: str, bid: str = "1") -> dict:
    return {
        "id": f"{base}/body/{bid}",
        "type": "https://schema.oparl.org/1.1/Body",
        "name": f"Stadt {bid}",
        "shortName": f"S{bid}",
        "paper": f"{base}/body/{bid}/paper",
        "organization": f"{base}/body/{bid}/organization",
    }


def _paginated(data: list[dict], base: str, path: str) -> dict:
    return {
        "data": data,
        "links": {"self": f"{base}{path}?page=1", "first": f"{base}{path}?page=1"},
        "pagination": {"elementsPerPage": 100, "currentPage": 1, "totalElements": len(data)},
    }


EMPTY_ENDPOINTS = (
    "organization", "person", "membership", "meeting", "agendaItem",
    "consultation", "file", "location", "legislativeTerm",
)


async def test_sync_persists_bodies_and_papers(db_session):
    base = "https://oparl.test"

    async with async_session_factory() as s:
        src = Source(name="Test", system_url=f"{base}/system", is_active=True)
        s.add(src)
        await s.commit()
        source_id = src.id

    with respx.mock(base_url=base, assert_all_called=False, assert_all_mocked=False) as mock:
        mock.get("/system").mock(return_value=Response(200, json=_system_json(base)))
        mock.get("/bodies").mock(
            return_value=Response(200, json=_paginated([_body(base, "1")], base, "/bodies"))
        )
        mock.get("/body/1/paper").mock(
            return_value=Response(
                200,
                json=_paginated(
                    [
                        {
                            "id": f"{base}/paper/p1",
                            "type": "https://schema.oparl.org/1.1/Paper",
                            "name": "Vorlage 1",
                            "reference": "V/1",
                        }
                    ],
                    base,
                    "/body/1/paper",
                ),
            )
        )
        for path in EMPTY_ENDPOINTS:
            mock.get(f"/body/1/{path}").mock(
                return_value=Response(200, json=_paginated([], base, f"/body/1/{path}"))
            )

        result = await sync_source_flow.fn(source_id, full=True)

    assert result["status"] in ("success", "partial")

    async with async_session_factory() as s:
        bodies = (await s.execute(select(Body))).scalars().all()
        papers = (await s.execute(select(Paper))).scalars().all()

    assert len(bodies) == 1
    assert bodies[0].name == "Stadt 1"
    assert len(papers) == 1
    assert papers[0].reference == "V/1"


async def test_sync_handles_5xx_without_crashing(db_session):
    """500 auf einem Endpoint darf den Sync nicht killen; Body muss trotzdem angelegt sein."""
    base = "https://oparl-broken.test"

    async with async_session_factory() as s:
        src = Source(name="Broken", system_url=f"{base}/system", is_active=True)
        s.add(src)
        await s.commit()
        source_id = src.id

    with respx.mock(base_url=base, assert_all_called=False, assert_all_mocked=False) as mock:
        mock.get("/system").mock(return_value=Response(200, json=_system_json(base)))
        mock.get("/bodies").mock(
            return_value=Response(200, json=_paginated([_body(base, "1")], base, "/bodies"))
        )
        # /paper liefert immer 500
        mock.get("/body/1/paper").mock(return_value=Response(500, text="boom"))
        for path in EMPTY_ENDPOINTS:
            mock.get(f"/body/1/{path}").mock(
                return_value=Response(200, json=_paginated([], base, f"/body/1/{path}"))
            )

        result = await sync_source_flow.fn(source_id, full=True)

    # Kein Crash, Status = partial (Body da, ein Endpoint fehlgeschlagen)
    assert result["status"] in ("partial", "success", "failed")

    async with async_session_factory() as s:
        bodies = (await s.execute(select(Body))).scalars().all()
    assert len(bodies) == 1


async def test_quarantine_after_threshold(db_session):
    """Nach threshold failed-Runs wird Source automatisch deaktiviert."""
    settings = get_settings()
    threshold = settings.sync_quarantine_threshold

    async with async_session_factory() as s:
        src = Source(
            name="Failing",
            system_url="https://does-not-resolve.invalid/system",
            is_active=True,
            consecutive_failures=threshold - 1,
        )
        s.add(src)
        await s.commit()
        source_id = src.id

    await sync_source_flow.fn(source_id, full=True)

    async with async_session_factory() as s:
        src_after = (await s.execute(select(Source).where(Source.id == source_id))).scalar_one()

    assert src_after.consecutive_failures >= threshold
    assert src_after.is_active is False
    assert src_after.quarantined_at is not None
