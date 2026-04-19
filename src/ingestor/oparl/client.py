"""
Async OParl-Client.

Funktionen:
- HTTP-Calls mit httpx (async, HTTP/2)
- Automatische Pagination via `next`-URL
- Incremental-Sync via `modified_since` Query-Parameter
- Retries mit Exponential Backoff (tenacity)
- OParl 1.0 + 1.1 Kompatibilität
- Rate-Limiting via Semaphore
- Strukturiertes Logging
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Self

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestor.config import get_settings

logger = logging.getLogger(__name__)


class OParlClientError(Exception):
    """Fehler beim Zugriff auf OParl-API."""


class OParlNotFoundError(OParlClientError):
    """OParl-Endpoint liefert 404."""


class OParlServerError(OParlClientError):
    """OParl-Server-Fehler (5xx)."""


class OParlClient:
    """
    Async-Client für OParl-Schnittstellen.

    Beispiel:
        async with OParlClient() as client:
            system = await client.fetch("https://oparl.example.de/system")
            async for body in client.list_paginated(system["body"]):
                ...
    """

    def __init__(
        self,
        timeout: float | None = None,
        max_concurrent: int = 10,
        user_agent: str | None = None,
    ) -> None:
        settings = get_settings()
        self._timeout = timeout or settings.http_timeout_seconds
        self._max_retries = settings.http_max_retries
        self._user_agent = user_agent or settings.http_user_agent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
            http2=True,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "application/json",
            },
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """
        Lädt eine OParl-URL und gibt das JSON-Objekt zurück.

        Nutzt Semaphore für Rate-Limiting + automatische Retries.
        """
        if not self._client:
            raise RuntimeError("Client not started — use 'async with'")

        async with self._semaphore:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retries),
                wait=wait_exponential(multiplier=1, min=1, max=10),
                retry=retry_if_exception_type((httpx.TransportError, OParlServerError)),
                reraise=True,
            ):
                with attempt:
                    return await self._do_fetch(url, params)
        raise OParlClientError(f"All retries exhausted for {url}")

    async def _do_fetch(self, url: str, params: dict[str, Any] | None) -> dict:
        assert self._client is not None
        try:
            response = await self._client.get(url, params=params)
        except httpx.TransportError as exc:
            logger.warning("Transport error %s: %s", url, exc)
            raise

        if response.status_code == 404:
            raise OParlNotFoundError(f"404 at {url}")
        if 500 <= response.status_code < 600:
            raise OParlServerError(f"HTTP {response.status_code} at {url}")
        if response.status_code >= 400:
            raise OParlClientError(f"HTTP {response.status_code} at {url}: {response.text[:200]}")

        # Encoding-Fix: Manche OParl-Server liefern UTF-8 ohne korrekten
        # Content-Type-Charset. httpx fällt dann auf latin-1 zurück.
        # Wir erzwingen UTF-8 (OParl-Spec schreibt JSON/UTF-8 vor).
        try:
            text = response.content.decode("utf-8")
        except UnicodeDecodeError:
            text = response.text  # Fallback auf httpx-Erkennung

        try:
            import json

            return json.loads(text)
        except ValueError as exc:
            raise OParlClientError(f"Invalid JSON from {url}: {exc}") from exc

    async def list_paginated(
        self,
        url: str,
        modified_since: datetime | None = None,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict]:
        """
        Iteriert über paginierte OParl-Listen.

        Folgt dem `next`-Link automatisch. Unterstützt sowohl OParl-Standard
        (`data` + `links.next`) als auch Single-Object-Antworten (Fallback).

        Args:
            url: Listen-Endpoint URL
            modified_since: Nur Objekte ab diesem Zeitpunkt (ISO-8601)
            max_pages: Optional, Limit gegen Endlosschleifen
        """
        params: dict[str, Any] = {}
        if modified_since:
            params["modified_since"] = modified_since.strftime("%Y-%m-%dT%H:%M:%S")

        page = 0
        current_url: str | None = url
        current_params: dict[str, Any] | None = params

        while current_url:
            page += 1
            if max_pages and page > max_pages:
                logger.warning("Max pages (%d) reached for %s", max_pages, url)
                break

            try:
                payload = await self.fetch(current_url, current_params)
            except OParlNotFoundError:
                logger.info("404 at %s — treating as empty list", current_url)
                return

            # OParl-Standard: data[] + links.next
            data = payload.get("data")
            if isinstance(data, list):
                for item in data:
                    yield item
                links = payload.get("links") or {}
                next_url = links.get("next")
                if next_url and next_url != current_url:
                    current_url = next_url
                    current_params = None  # next-URL hat schon Parameter
                else:
                    break
                continue

            # Fallback: Single-Object-Response (z.B. ITK Rheinland)
            if isinstance(payload, dict) and payload.get("type"):
                yield payload
                return

            # Unbekannte Struktur
            logger.warning("Unknown response structure at %s", current_url)
            return

    async def fetch_system(self, system_url: str) -> dict:
        """Lädt den System-Endpoint einer OParl-Quelle."""
        return await self.fetch(system_url)

    async def fetch_bodies(self, system: dict) -> AsyncIterator[dict]:
        """Iteriert über die Bodies eines Systems."""
        body_list_url = system.get("body")
        if not body_list_url:
            return
        async for body in self.list_paginated(body_list_url):
            yield body
