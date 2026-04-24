"""Tests für Health/Liveness/Readiness-Endpoints (httpx.AsyncClient)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from ingestor.api.main import app

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_liveness_always_ok(client):
    r = await client.get("/health/live")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


async def test_health_reports_db_status(client, monkeypatch):
    async def fake_ok() -> bool:
        return True

    from ingestor.api import main as main_module

    monkeypatch.setattr(main_module, "check_db", fake_ok)

    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["db"] == "ok"


async def test_health_503_when_db_down(client, monkeypatch):
    async def fake_fail() -> bool:
        return False

    from ingestor.api import main as main_module

    monkeypatch.setattr(main_module, "check_db", fake_fail)

    r = await client.get("/health")
    assert r.status_code == 503


async def test_ready_503_when_db_down(client, monkeypatch):
    async def fake_fail() -> bool:
        return False

    from ingestor.api import main as main_module

    monkeypatch.setattr(main_module, "check_db", fake_fail)

    r = await client.get("/health/ready")
    assert r.status_code == 503
    assert r.json()["ready"] is False
