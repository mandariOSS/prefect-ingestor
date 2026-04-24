"""Tests für die neuen Management-Endpoints (httpx.AsyncClient)."""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select

from ingestor.api.auth import generate_api_key, hash_api_key, key_prefix
from ingestor.api.main import app
from ingestor.db import async_session_factory
from ingestor.db.models import ApiKey, AuditLog, Body, File, Source

pytestmark = [pytest.mark.asyncio, pytest.mark.db]


@pytest_asyncio.fixture
async def client(db_session) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _admin_key() -> str:
    plaintext = generate_api_key()
    async with async_session_factory() as s:
        s.add(
            ApiKey(
                name="admin",
                key_hash=hash_api_key(plaintext),
                prefix=key_prefix(plaintext),
                scopes="admin",
                created_by="test",
            )
        )
        await s.commit()
    return plaintext


# =============================================================================
# Quarantäne
# =============================================================================


async def test_unquarantine_resets_state(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        src = Source(
            name="Q",
            system_url="https://q.test/system",
            is_active=False,
            consecutive_failures=12,
            quarantined_at=datetime.now(UTC),
            last_error="viele Fehler",
        )
        s.add(src)
        await s.commit()
        sid = src.id

    r = await client.post(f"/api/admin/sources/{sid}/unquarantine", headers={"X-API-Key": key})
    assert r.status_code == 200
    body = r.json()
    assert body["is_active"] is True
    assert body["consecutive_failures"] == 0
    assert body["quarantined_at"] is None
    assert body["last_error"] is None


# =============================================================================
# Bulk
# =============================================================================


async def test_bulk_activate_changes_multiple(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        s1 = Source(name="A", system_url="https://a.test/system", is_active=False)
        s2 = Source(name="B", system_url="https://b.test/system", is_active=False)
        s.add_all([s1, s2])
        await s.commit()
        ids = [str(s1.id), str(s2.id), str(uuid4())]

    r = await client.post(
        "/api/admin/sources/bulk-activate",
        headers={"X-API-Key": key},
        json={"source_ids": ids},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["affected"] == 2
    assert len(body["not_found"]) == 1


# =============================================================================
# CSV-Import
# =============================================================================


async def test_csv_import_inserts_and_skips_duplicates(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        s.add(Source(name="Dup", system_url="https://dup.test/system"))
        await s.commit()

    csv_content = (
        "name,system_url\n"
        "Neu 1,https://neu1.test/system\n"
        "Neu 2,https://neu2.test/system\n"
        "Dup,https://dup.test/system\n"
        ",https://invalid.test/system\n"
        "Bad Scheme,file:///etc/passwd\n"
    )

    r = await client.post(
        "/api/admin/sources/import",
        headers={"X-API-Key": key},
        files={"file": ("src.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["inserted"] == 2
    assert body["skipped_duplicate"] == 1
    assert body["invalid_rows"] == 2


# =============================================================================
# Per-Source-Stats
# =============================================================================


async def test_source_stats_returns_counts(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        src = Source(name="Stats", system_url="https://stats.test/system")
        s.add(src)
        await s.commit()
        sid = src.id

    r = await client.get(f"/api/admin/sources/{sid}/stats", headers={"X-API-Key": key})
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == str(sid)
    assert body["bodies"] == 0
    assert body["files_pending"] == 0


# =============================================================================
# OCR-Retry
# =============================================================================


async def test_retry_ocr_resets_status(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        src = Source(name="F", system_url="https://f.test/system")
        s.add(src)
        await s.flush()
        body = Body(
            source_id=src.id,
            external_id="https://f.test/body/1",
            name="B",
            raw={"id": "x", "type": "Body"},
        )
        s.add(body)
        await s.flush()
        f = File(
            body_id=body.id,
            external_id="https://f.test/file/1",
            text_extraction_status="failed",
            text_extraction_error="boom",
            raw={"id": "y"},
        )
        s.add(f)
        await s.commit()
        fid = f.id

    r = await client.post(f"/api/admin/files/{fid}/retry-ocr", headers={"X-API-Key": key})
    assert r.status_code == 200
    assert r.json()["previous_status"] == "failed"
    assert r.json()["new_status"] == "pending"

    async with async_session_factory() as s:
        refreshed = (await s.execute(select(File).where(File.id == fid))).scalar_one()
    assert refreshed.text_extraction_status == "pending"
    assert refreshed.text_extraction_error is None


async def test_retry_all_failed_ocr(client):
    key = await _admin_key()

    async with async_session_factory() as s:
        src = Source(name="G", system_url="https://g.test/system")
        s.add(src)
        await s.flush()
        body = Body(source_id=src.id, external_id="x", name="B", raw={})
        s.add(body)
        await s.flush()
        for i in range(3):
            s.add(
                File(
                    body_id=body.id,
                    external_id=f"u{i}",
                    text_extraction_status="failed",
                    raw={},
                )
            )
        await s.commit()

    r = await client.post("/api/admin/files/retry-failed-ocr", headers={"X-API-Key": key})
    assert r.status_code == 200
    assert r.json()["reset_count"] == 3


# =============================================================================
# API-Keys CRUD
# =============================================================================


async def test_create_api_key_returns_plaintext_once(client):
    admin = await _admin_key()

    r = await client.post(
        "/api/admin/api-keys",
        headers={"X-API-Key": admin},
        json={"name": "new-read-key", "scopes": ["read"]},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["plaintext_key"].startswith("mi_")
    assert body["key"]["prefix"] == body["plaintext_key"][:8]
    assert body["key"]["scopes"] == ["read"]

    new_key = body["plaintext_key"]
    r2 = await client.get("/api/admin/whoami", headers={"X-API-Key": new_key})
    assert r2.status_code == 200
    assert r2.json()["actor"] == "new-read-key"


async def test_list_api_keys_contains_created(client):
    admin = await _admin_key()
    await client.post(
        "/api/admin/api-keys",
        headers={"X-API-Key": admin},
        json={"name": "listed", "scopes": ["read"]},
    )
    r = await client.get("/api/admin/api-keys", headers={"X-API-Key": admin})
    assert r.status_code == 200
    assert "listed" in [k["name"] for k in r.json()]


async def test_revoke_api_key_blocks_access(client):
    admin = await _admin_key()

    created = (await client.post(
        "/api/admin/api-keys",
        headers={"X-API-Key": admin},
        json={"name": "to-revoke", "scopes": ["read"]},
    )).json()
    new_key = created["plaintext_key"]
    new_id = created["key"]["id"]

    assert (await client.get("/api/admin/whoami", headers={"X-API-Key": new_key})).status_code == 200

    r = await client.delete(f"/api/admin/api-keys/{new_id}", headers={"X-API-Key": admin})
    assert r.status_code == 204

    assert (await client.get("/api/admin/whoami", headers={"X-API-Key": new_key})).status_code == 401


# =============================================================================
# Audit-Log
# =============================================================================


async def test_audit_log_records_actions(client):
    key = await _admin_key()

    r = await client.post(
        "/api/admin/sources",
        headers={"X-API-Key": key},
        json={"name": "AuditTest", "system_url": "https://audit.test/system"},
    )
    assert r.status_code == 201

    async with async_session_factory() as s:
        logs = (await s.execute(select(AuditLog).order_by(AuditLog.timestamp.desc()))).scalars().all()
    actions = [l.action for l in logs]
    assert "source.create" in actions
    creation = next(l for l in logs if l.action == "source.create")
    assert creation.actor == "admin"
    assert creation.resource_type == "source"


async def test_audit_log_endpoint_requires_admin(client):
    plaintext = generate_api_key()
    async with async_session_factory() as s:
        s.add(
            ApiKey(
                name="writer",
                key_hash=hash_api_key(plaintext),
                prefix=key_prefix(plaintext),
                scopes="write",
                created_by="t",
            )
        )
        await s.commit()

    r = await client.get("/api/admin/audit-logs", headers={"X-API-Key": plaintext})
    assert r.status_code == 403


async def test_audit_log_filter_by_action(client):
    key = await _admin_key()

    await client.post(
        "/api/admin/sources",
        headers={"X-API-Key": key},
        json={"name": "A1", "system_url": "https://a1.test/system"},
    )
    await client.get("/api/admin/stats", headers={"X-API-Key": key})

    r = await client.get(
        "/api/admin/audit-logs?action=source.create", headers={"X-API-Key": key}
    )
    assert r.status_code == 200
    for entry in r.json():
        assert entry["action"] == "source.create"
