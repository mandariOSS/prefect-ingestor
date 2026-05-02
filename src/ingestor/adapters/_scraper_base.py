"""
ScraperAdapterBase — gemeinsame Helfer für HTML-scrapende RIS-Adapter.

Konkret-Implementations (SessionNetAdapter, AllRISAdapter, SDNetAdapter)
erben hiervon und müssen nur die vendor-spezifischen Selektoren + URL-
Patterns liefern.

Geteilte Funktionalität:
    - Async httpx Client mit korrekten Defaults für deutsche Behörden-Server
    - User-Agent identifiziert Mandari klar (gute Netiquette)
    - Optional Cookie-Jar (manche RIS verlangen Session-Initialisierung)
    - Selectolax als HTML-Parser (Faktor-3-schneller als bs4 für Tabellen)
    - Sleep zwischen Requests (rate_limit_seconds-Setting pro Adapter)

Output-Kontrakt: Jede Konkrete-Implementation MUSS OParl-1.1-shape-JSON-
dicts liefern. Mapping-Referenzen pro Vendor stehen in den jeweiligen
Skeleton-Files.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Self

import httpx

from ingestor.adapters.base import BaseAdapter
from ingestor.config import get_settings

logger = logging.getLogger(__name__)


class ScraperAdapterBase(BaseAdapter):
    """Gemeinsame Basis für alle HTML-scrapenden Adapter.

    Subclasses MÜSSEN ``ADAPTER_TYPE`` setzen und die abstrakten Methoden
    aus ``BaseAdapter`` implementieren.
    """

    #: Pause zwischen zwei HTTP-Requests an dasselbe RIS. Default 1 s ist
    #: sehr defensiv — die meisten kommunalen RIS-Server vertragen das
    #: locker, aber wir spielen lieber sicher (war eh nie das Bottleneck,
    #: weil wir nur alle paar Stunden syncen).
    REQUEST_INTERVAL_SECONDS: float = 1.0

    def __init__(self, source) -> None:  # noqa: ANN001
        super().__init__(source)
        self._client: httpx.AsyncClient | None = None
        self._last_request_at: float = 0.0

    async def __aenter__(self) -> Self:
        settings = get_settings()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            follow_redirects=True,
            http2=False,  # viele Behörden-Apache-Konfigurationen droppen HTTP/2
            headers={
                "User-Agent": settings.http_user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
            },
            # Cookie-Jar enabled by default — manche RIS setzen Session-Cookies
            # auf der Landing-Page, ohne die spätere Requests 403 liefern.
            cookies={},
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _throttle(self) -> None:
        """Sicherstellt min. ``REQUEST_INTERVAL_SECONDS`` zwischen Requests."""
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_at
        if elapsed < self.REQUEST_INTERVAL_SECONDS:
            await asyncio.sleep(self.REQUEST_INTERVAL_SECONDS - elapsed)
        self._last_request_at = asyncio.get_event_loop().time()

    async def _get_html(self, url: str, params: dict | None = None) -> str:
        """Lädt eine HTML-Seite mit Throttling.

        Wirft RuntimeError wenn nicht im async-with-Context.
        Wirft httpx.HTTPError bei 4xx/5xx.
        """
        if self._client is None:
            raise RuntimeError("Adapter not entered — use 'async with'")
        await self._throttle()
        response = await self._client.get(url, params=params)
        response.raise_for_status()
        return response.text
