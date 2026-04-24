"""Gemeinsame FastAPI-Dependencies (DB-Session)."""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor.db import async_session_factory


async def db_session() -> AsyncIterator[AsyncSession]:
    """DB-Session pro Request."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


SessionDep = Annotated[AsyncSession, Depends(db_session)]
