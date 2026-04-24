"""
Pytest-Fixtures für den Ingestor.

Setup:
- Schema wird vor dem Test-Run einmal erstellt (via sync-create_all auf
  dem Sync-Engine — vermeidet Event-Loop-Kollisionen mit pytest-asyncio).
- Zwischen Tests: TRUNCATE CASCADE statt drop/create (~100× schneller).

DB: via DATABASE_URL (Default: localhost test-DB).
"""

from __future__ import annotations

import os

# Muss vor ALLEN ingestor-Imports: session.py liest diese Env-Var bei Modul-Load
os.environ.setdefault("INGESTOR_TEST_MODE", "1")

from collections.abc import AsyncIterator  # noqa: E402

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from ingestor.config import get_settings  # noqa: E402
from ingestor.db.models import Base  # noqa: E402
from ingestor.db.session import async_session_factory, engine  # noqa: E402


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("TEST_SKIP_DB"):
        skip = pytest.mark.skip(reason="TEST_SKIP_DB gesetzt")
        for item in items:
            if "db" in item.keywords:
                item.add_marker(skip)


def _setup_schema_sync() -> None:
    """Synchron Schema erstellen — umgeht jede Event-Loop-Kollision."""
    sync_url = get_settings().database_url.replace(
        "postgresql+asyncpg://", "postgresql+psycopg://"
    )
    sync_engine = create_engine(sync_url, echo=False)
    try:
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
    finally:
        sync_engine.dispose()


# Einmalig, bevor irgendein Test läuft
_setup_schema_sync()


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncIterator:
    """TRUNCATE vor jedem Test. Dank NullPool (INGESTOR_TEST_MODE=1) keine Pool-Probleme."""
    table_names = [t.name for t in Base.metadata.sorted_tables]
    async with engine.begin() as conn:
        await conn.execute(
            text(f'TRUNCATE {", ".join(table_names)} RESTART IDENTITY CASCADE')
        )

    async with async_session_factory() as session:
        yield session
        await session.rollback()
