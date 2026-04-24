"""SQLAlchemy Async-Session-Factory.

Pool-Settings sind auf Robustheit ausgelegt:
- pool_pre_ping: prüft jede Connection vor Verwendung (fängt Reconnect-Szenarien
  nach Netzwerk-Abbrüchen/Postgres-Restarts)
- pool_recycle: recycelt Connections nach N Sekunden (schützt vor stillen
  Idle-Timeouts von Firewalls/Connection-Poolern)
- pool_timeout: wartet maximal N Sekunden auf eine freie Connection
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from ingestor.config import get_settings

_settings = get_settings()

# Test-Mode: NullPool vermeidet Cross-Event-Loop-Connections (pytest-asyncio
# erzeugt pro Test einen frischen Loop — ein Pool würde Verbindungen halten,
# die an einen bereits geschlossenen Loop gebunden sind).
if os.environ.get("INGESTOR_TEST_MODE") == "1":
    engine = create_async_engine(
        _settings.database_url,
        poolclass=NullPool,
        echo=False,
    )
else:
    engine = create_async_engine(
        _settings.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
        echo=False,
    )

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    """Context-Manager für eine DB-Session."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_db() -> bool:
    """Führt SELECT 1 aus — True bei Erfolg, False bei DB-Problem."""
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
