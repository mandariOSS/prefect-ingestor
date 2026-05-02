"""
OParlAdapter — wraps the existing async OParlClient as a BaseAdapter.

Dies ist die einzige produktionsreife Adapter-Implementation (Stand v0.2).
Funktioniert für alle ~147 OParl-1.0/1.1-Endpoints, die in Deutschland live
sind (siehe github.com/OParl/resources).

Implementierungsdetails:
    - HTTP-Client mit HTTP/2, follow_redirects, retries via tenacity
    - Pagination: folgt ``links.next`` automatisch
    - Incremental: nutzt ``?modified_since=`` Query-Parameter (OParl-Spec)
    - Rate-Limiting: Semaphore (default 10 concurrent requests pro Source)

Diese Klasse ändert NICHT das Verhalten des bestehenden OParlClient — sie
ist ein dünner Wrapper, damit Sync-Flow und Adapter-Registry einheitlich
arbeiten können.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Self

from ingestor.adapters.base import (
    AdapterFetchError,
    BaseAdapter,
    EntityType,
)
from ingestor.oparl import OParlClient

logger = logging.getLogger(__name__)


class OParlAdapter(BaseAdapter):
    """OParl-1.0/1.1-konformer Adapter.

    Nutzt ``source.system_url`` als Discovery-Einstieg.
    Ignoriert ``source.config`` (für OParl gibt es keine vendor-spezifischen
    Settings — die OParl-Spec definiert alles).
    """

    ADAPTER_TYPE = "oparl"

    def __init__(self, source) -> None:  # noqa: ANN001
        super().__init__(source)
        self._client: OParlClient | None = None
        self._system_doc: dict | None = None

    async def __aenter__(self) -> Self:
        self._client = OParlClient()
        await self._client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.__aexit__(exc_type, exc, tb)
            self._client = None

    def supports_incremental(self) -> bool:
        return True

    async def discover_bodies(self) -> AsyncIterator[dict]:
        """Lädt das System und yieldet alle Bodies."""
        if self._client is None:
            raise RuntimeError("Adapter not entered — use 'async with'")

        logger.info("OParl discover_bodies for %s", self.source.system_url)
        self._system_doc = await self._client.fetch_system(self.source.system_url)
        async for body in self._client.fetch_bodies(self._system_doc):
            yield body

    async def list_entities(
        self,
        body: dict,
        entity_type: EntityType,
        modified_since: datetime | None = None,
    ) -> AsyncIterator[dict]:
        """Iteriert über alle Entities eines Body-Endpoints.

        Holt die URL aus ``body[entity_type.value]`` (OParl-Spec definiert
        das so). Wenn das Feld fehlt → leere Iteration (kein Fehler).
        """
        if self._client is None:
            raise RuntimeError("Adapter not entered — use 'async with'")

        url = body.get(entity_type.value)
        if not url:
            logger.debug(
                "Body %s has no '%s' endpoint", body.get("shortName"), entity_type.value
            )
            return

        try:
            async for item in self._client.list_paginated(
                url, modified_since=modified_since
            ):
                yield item
        except Exception as exc:  # noqa: BLE001
            raise AdapterFetchError(
                f"OParl list_entities({entity_type.value}) failed for "
                f"{body.get('shortName')}: {exc}"
            ) from exc

    async def get_entity(self, url: str) -> dict | None:
        """Lädt eine einzelne Entity per URL nach (für Embedded-Resolution)."""
        if self._client is None:
            raise RuntimeError("Adapter not entered")
        try:
            return await self._client.fetch(url)
        except Exception as exc:  # noqa: BLE001
            logger.debug("OParl get_entity(%s) failed: %s", url, exc)
            return None
