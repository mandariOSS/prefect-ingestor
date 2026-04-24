"""Test für den Source-Seed."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select

from ingestor.db import async_session_factory
from ingestor.db.models import Source
from ingestor.workers.seed import seed_sources_if_empty

pytestmark = [pytest.mark.asyncio, pytest.mark.db]


async def test_seed_inserts_from_csv(db_session, tmp_path: Path):
    csv = tmp_path / "src.csv"
    csv.write_text(
        "name,system_url\nStadt A,https://a.test/system\nStadt B,https://b.test/system\n",
        encoding="utf-8",
    )

    result = await seed_sources_if_empty(csv)
    assert result == {"inserted": 2, "path": str(csv)}

    async with async_session_factory() as s:
        sources = (await s.execute(select(Source))).scalars().all()

    assert {src.name for src in sources} == {"Stadt A", "Stadt B"}
    assert all(not src.is_active for src in sources)


async def test_seed_skips_if_db_not_empty(db_session, tmp_path: Path):
    async with async_session_factory() as s:
        s.add(Source(name="Existing", system_url="https://existing.test/system"))
        await s.commit()

    csv = tmp_path / "src.csv"
    csv.write_text("name,system_url\nNew,https://new.test/system\n", encoding="utf-8")

    result = await seed_sources_if_empty(csv)
    assert result == {"skipped": "db not empty", "existing": 1}
