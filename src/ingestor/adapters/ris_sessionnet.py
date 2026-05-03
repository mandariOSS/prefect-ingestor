"""
SessionNetAdapter — HTML-Scraper für Somacos SessionNet / Mandatos.

Vendor-Übersicht:
    SOMACOS Software- und Beratungsgesellschaft mbH (Hamburg)
    https://www.somacos.de/loesungen/sessionnet/
    https://www.akdb.de/loesungen/digitale-verwaltung/session-sessionnet-mandatos/

    Einer der drei Marktführer im deutschen RIS-Markt. ~2 000 Installationen,
    AKDB (Bayern) ist der größte Reseller. SessionNet ist die klassische
    Hosted-Variante, "Session" die On-Premise-Variante mit gleicher Codebase.
    "Mandatos" ist die mobile Mandatsträger-App.

OParl-Status:
    SOMACOS hat OParl-1.0 als optionales Modul Anfang 2017 ausgeliefert
    (https://somacos.de/somacos-neue-schnittstelle-sessionnet-oparl-1-0/).
    Aktivierung muss pro Mandant separat freigeschaltet werden — viele
    Kommunen haben das Modul lizenziert aber nie aktiviert. Wo OParl
    aktiv ist, NUTZE den OParlAdapter. Dieser Scraper greift nur, wenn
    SessionNet OHNE OParl-Modul läuft.

URL-Pattern (modernes SessionNet V:050500+, Stand 2026):
    https://{host}/sessionnet/sessionnetbi/                      → Landing-Redirect
    https://{host}/sessionnet/sessionnetbi/info.php              → Übersicht
    https://{host}/sessionnet/sessionnetbi/si0040.php            → Sitzungs-Liste
    https://{host}/sessionnet/sessionnetbi/si0057.php?__ksinr=N  → Sitzungs-Detail
    https://{host}/sessionnet/sessionnetbi/vo0040.php            → Vorlagen-Übersicht
    https://{host}/sessionnet/sessionnetbi/do0040.php            → Drucksachen
    https://{host}/sessionnet/sessionnetbi/gr0040.php            → Gremien-Übersicht
    https://{host}/sessionnet/sessionnetbi/kp0041.php            → Personen-Übersicht
    https://{host}/sessionnet/sessionnetbi/getfile.php?id=N      → File-Download

    Älteres SessionNet (vor V:050500) nutzt ``.asp``-Endpoints mit gleichen
    Stamm-Namen (sitzungslang.asp, info.asp, vorgang.asp, …) — wenn das
    relevant wird, in ``adapters/sessionnet/_html.py:resolve()`` Fallback
    einbauen.

Source-Configuration (``Source.config`` JSONB):
    {
        "base_url": "https://www.stadt-muenster.de/sessionnet/sessionnetbi",
        "name": "Stadt Münster",          # optional, sonst Title-Scrape
        "short_name": "Münster",          # optional, sonst Hostname-Ableitung
        "include_details": false          # default; true holt si0057-Detail
                                          # pro Meeting (langsamer, mehr TOPs)
    }

ENTWICKLUNGS-STATUS (v0.2):
    ✅ discover_bodies         — synthetisierter Body aus Config + Title-Scrape
    ✅ list_entities(MEETING)  — si0040.php → Liste mit Anhängen, Datum, Ort
    ⏳ list_entities(ORGANIZATION) — TODO (gr0040.php)
    ⏳ list_entities(PERSON)   — TODO (kp0041.php)
    ⏳ list_entities(PAPER)    — TODO (vo0040.php / do0040.php)
    ⏳ list_entities(FILE)     — wird inline mit Meetings/Papers extrahiert
    ⏳ list_entities(MEMBERSHIP / AGENDA_ITEM / CONSULTATION)
                                — Embedded in Detail-Pages, brauchen
                                  include_details=true im Config

Meeting-Scraper validiert gegen:
    - https://www.stadt-muenster.de/sessionnet/sessionnetbi/  (V:050500)

Mapping-Referenzen für noch zu implementierende Typen:
    - politik-bei-uns/scrape-a-ris (Python 2, abandoned 2016) als Konzept
    - github.com/OpenRuhr/ris-scraper (2018) — etwas neuer, gleiche Familie
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime

from ingestor.adapters._scraper_base import ScraperAdapterBase
from ingestor.adapters.base import AdapterError, EntityType
from ingestor.adapters.sessionnet import bodies as bodies_mod
from ingestor.adapters.sessionnet import meetings as meetings_mod

logger = logging.getLogger(__name__)


class SessionNetAdapter(ScraperAdapterBase):
    """Scraper für Somacos SessionNet / Mandatos.

    Status: meetings produktiv, restliche Entity-Types als TODO im Modul-
    Docstring dokumentiert.
    """

    ADAPTER_TYPE = "ris_sessionnet"
    REQUEST_INTERVAL_SECONDS = 1.5  # defensiv — Mandanten teilen sich oft Server

    # ─────────────────────── Config-Resolution ───────────────────────

    def _base_url(self) -> str:
        cfg = self.source.config or {}
        base = cfg.get("base_url") or self.source.system_url
        if not base:
            raise AdapterError(
                f"SessionNet source {self.source.name!r} has no base_url "
                "(set source.config.base_url)"
            )
        return base.rstrip("/")

    # ─────────────────────── Discovery ───────────────────────

    async def discover_bodies(self) -> AsyncIterator[dict]:
        if self._client is None:
            raise RuntimeError("Adapter not entered — use 'async with'")
        await self._throttle()
        async for body in bodies_mod.discover(
            self._client, self._base_url(), self.source.config or {}
        ):
            yield body

    # ─────────────────────── Listing ───────────────────────

    async def list_entities(
        self,
        body: dict,
        entity_type: EntityType,
        modified_since: datetime | None = None,
    ) -> AsyncIterator[dict]:
        if self._client is None:
            raise RuntimeError("Adapter not entered — use 'async with'")

        base = self._base_url()

        if entity_type == EntityType.MEETING:
            await self._throttle()
            async for meeting in meetings_mod.list_meetings(
                self._client, base, modified_since=modified_since
            ):
                # body_id wird vom downstream-upsert gesetzt — wir hängen
                # ihn nicht ins JSON, das ist Sync-Flow-Verantwortung.
                yield meeting
            return

        # Alle anderen Entity-Types: noch nicht implementiert.
        # Wir werfen NotImplementedError, der Sync-Flow behandelt das als
        # "0 Items, kein Fehler" (siehe sync_body_endpoint).
        raise NotImplementedError(
            f"SessionNetAdapter.list_entities({entity_type.value}) — "
            "siehe TODOs im Modul-Docstring"
        )
        yield  # type: ignore[unreachable]
