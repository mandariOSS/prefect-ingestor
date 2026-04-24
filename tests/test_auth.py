"""
Tests für die Management-API-Authentication + Authorization.

Verwendet httpx.AsyncClient + ASGITransport — läuft im gleichen Event-Loop
wie die Fixtures, vermeidet "Task attached to different loop".
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select

from ingestor.api.auth import generate_api_key, hash_api_key, key_prefix
from ingestor.api.main import app
from ingestor.db import async_session_factory
from ingestor.db.models import ApiKey

pytestmark = [pytest.mark.asyncio, pytest.mark.db]


@pytest_asyncio.fixture
async def client(db_session) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _create_db_key(name: str, scopes: str = "read") -> str:
    plaintext = generate_api_key()
    async with async_session_factory() as s:
        s.add(
            ApiKey(
                name=name,
                key_hash=hash_api_key(plaintext),
                prefix=key_prefix(plaintext),
                scopes=scopes,
                created_by="test",
            )
        )
        await s.commit()
    return plaintext


async def test_missing_header_returns_401(client):
    r = await client.get("/api/admin/sources")
    assert r.status_code == 401


async def test_wrong_key_returns_401(client):
    r = await client.get("/api/admin/sources", headers={"X-API-Key": "mi_nonsense_that_is_long_enough"})
    assert r.status_code == 401


async def test_bootstrap_key_works_when_db_empty(client, monkeypatch):
    from ingestor.config import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("API_KEY", "a_real_production_key_not_default_xyz")
    monkeypatch.setenv("DEV_MODE", "false")

    r = await client.get(
        "/api/admin/sources",
        headers={"X-API-Key": "a_real_production_key_not_default_xyz"},
    )
    assert r.status_code == 200
    get_settings.cache_clear()


async def test_bootstrap_rejected_when_db_key_exists(client, monkeypatch):
    from ingestor.config import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("API_KEY", "some_bootstrap_key_xyz_long_enough")
    monkeypatch.setenv("DEV_MODE", "false")

    await _create_db_key("prod-key", scopes="admin")

    r = await client.get(
        "/api/admin/sources",
        headers={"X-API-Key": "some_bootstrap_key_xyz_long_enough"},
    )
    assert r.status_code == 401
    get_settings.cache_clear()


async def test_read_scope_allows_get_denies_write(client):
    key = await _create_db_key("read-only", scopes="read")

    r = await client.get("/api/admin/sources", headers={"X-API-Key": key})
    assert r.status_code == 200

    r = await client.post(
        "/api/admin/sources",
        headers={"X-API-Key": key},
        json={"name": "X", "system_url": "https://x.test/system"},
    )
    assert r.status_code == 403


async def test_write_scope_implies_read(client):
    key = await _create_db_key("writer", scopes="write")
    r = await client.get("/api/admin/sources", headers={"X-API-Key": key})
    assert r.status_code == 200


async def test_admin_scope_implies_write(client):
    key = await _create_db_key("admin", scopes="admin")
    r = await client.post(
        "/api/admin/sources",
        headers={"X-API-Key": key},
        json={"name": "A", "system_url": "https://a-admin.test/system"},
    )
    assert r.status_code == 201


async def test_write_scope_cannot_manage_keys(client):
    key = await _create_db_key("writer", scopes="write")
    r = await client.get("/api/admin/api-keys", headers={"X-API-Key": key})
    assert r.status_code == 403


async def test_revoked_key_rejected(client):
    key = await _create_db_key("will-be-revoked", scopes="admin")
    prefix = key_prefix(key)

    async with async_session_factory() as s:
        ak = (await s.execute(select(ApiKey).where(ApiKey.prefix == prefix))).scalar_one()
        ak.revoked_at = datetime.now(UTC)
        await s.commit()

    r = await client.get("/api/admin/sources", headers={"X-API-Key": key})
    assert r.status_code == 401


async def test_expired_key_rejected(client):
    key = await _create_db_key("expired", scopes="admin")
    prefix = key_prefix(key)

    async with async_session_factory() as s:
        ak = (await s.execute(select(ApiKey).where(ApiKey.prefix == prefix))).scalar_one()
        ak.expires_at = datetime.now(UTC) - timedelta(hours=1)
        await s.commit()

    r = await client.get("/api/admin/sources", headers={"X-API-Key": key})
    assert r.status_code == 401


async def test_last_used_updated(client):
    key = await _create_db_key("tracker", scopes="read")
    prefix = key_prefix(key)

    r = await client.get("/api/admin/sources", headers={"X-API-Key": key})
    assert r.status_code == 200

    async with async_session_factory() as s:
        ak = (await s.execute(select(ApiKey).where(ApiKey.prefix == prefix))).scalar_one()
    assert ak.last_used_at is not None


async def test_whoami_returns_scopes(client):
    key = await _create_db_key("me", scopes="write")
    r = await client.get("/api/admin/whoami", headers={"X-API-Key": key})
    assert r.status_code == 200
    body = r.json()
    assert body["actor"] == "me"
    assert set(body["scopes"]) & {"read", "write", "admin"}


async def test_http_scheme_enforced_on_create(client):
    key = await _create_db_key("admin", scopes="admin")
    r = await client.post(
        "/api/admin/sources",
        headers={"X-API-Key": key},
        json={"name": "Bad", "system_url": "file:///etc/passwd"},
    )
    assert r.status_code == 422
