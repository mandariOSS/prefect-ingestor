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

URL-Pattern (Erkennungsmuster):
    https://{host}/bi/                                  → Landing-Page
    https://{host}/bi/sitzungslang.asp?MM=...&YY=...    → Sitzungs-Liste
    https://{host}/bi/info.asp?__ksinr=...              → Sitzungs-Detail
    https://{host}/bi/vorgang.asp?__cs_vid=...          → Vorlage-Detail
    https://{host}/bi/personenuebersicht.asp            → Personen-Liste
    https://{host}/bi/personendetail.asp?ID=...         → Personen-Detail

    Klassisches Microsoft IIS + ASP.NET Stack, query-string-driven.
    Stabile URL-Patterns über die letzten 10+ Jahre — Mapping-Code aus
    politik-bei-uns/scrape-a-ris (Python 2, 2016) ist konzeptionell noch
    valide.

Mapping-Referenz (HTML → OParl):
    NICHT als formales Schema dokumentiert. Die de-facto-Mapping liegt im
    Code von:
        github.com/politik-bei-uns/politik-bei-uns-scraper-old/
            tree/master/scraper/sessionnet
        github.com/marians/scrape-a-ris/tree/master/scraper

    Kernmappings:
        SessionNet "Sitzung"         → OParl Meeting
        SessionNet "TOP"             → OParl AgendaItem
        SessionNet "Vorlage"         → OParl Paper
        SessionNet "Beschluss"       → OParl Consultation
        SessionNet "Mitglied"        → OParl Person
        SessionNet "Gremium"         → OParl Organization
        SessionNet "Sitzungsraum"    → OParl Location
        SessionNet "Drucksache" PDF  → OParl File

Source-Configuration (``Source.config`` JSONB):
    {
        "base_url": "https://buergerinfo.example.de/bi",   # required
        "mandant_id": "1",                                  # optional
        "use_https": true,                                  # default true
        "encoding": "iso-8859-1"                            # default; manche
                                                            # auf utf-8 manuell
    }

ENTWICKLUNGS-STATUS: SKELETON.
Die Methoden werfen NotImplementedError mit konkreten TODOs. Implementierung
folgt iterativ, sobald der erste Kunde eine Nicht-OParl-SessionNet-Quelle
einbringt.

Implementierungsreihenfolge (empfohlen):
    1. ``discover_bodies`` — meistens 1 Body pro Source, aus Config-Hint
    2. ``list_entities(MEETING)`` — sitzungslang.asp paginiert über Monate
    3. ``list_entities(AGENDA_ITEM)`` — pro Sitzung info.asp parsen
    4. ``list_entities(PAPER)`` — vorgang.asp + Drucksachen-Liste
    5. ``list_entities(PERSON)`` — personenuebersicht.asp
    6. ``list_entities(ORGANIZATION)`` — Gremien-Liste (oft im Sidebar-Menü)
    7. ``list_entities(MEMBERSHIP)`` — Person→Gremium über Personendetail
    8. ``list_entities(FILE)`` — PDF-Anhänge aus Sitzungs-Details extrahieren
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime

from ingestor.adapters._scraper_base import ScraperAdapterBase
from ingestor.adapters.base import EntityType

logger = logging.getLogger(__name__)


class SessionNetAdapter(ScraperAdapterBase):
    """Scraper für Somacos SessionNet / Mandatos. SKELETON."""

    ADAPTER_TYPE = "ris_sessionnet"

    #: SessionNet ist relativ tolerant gegenüber Burst-Requests, aber wir
    #: bleiben defensiv — viele Mandanten teilen sich einen Hosting-Server.
    REQUEST_INTERVAL_SECONDS = 1.5

    async def discover_bodies(self) -> AsyncIterator[dict]:
        """TODO: Aus ``source.config["base_url"]`` einen Body synthetisieren.

        SessionNet hat typischerweise 1 Body pro Mandant — der Mandant ist
        identisch mit der Source. Felder:
            id           → ``{base_url}#body``
            type         → "https://schema.oparl.org/1.1/Body"
            shortName    → aus config["short_name"] oder Hostname-Ableitung
            name         → von Landing-Page-Title scrapen
            meeting/paper/person/...  → konstruierte Endpoint-URLs (s. URL-Patterns)
        """
        raise NotImplementedError(
            "SessionNetAdapter.discover_bodies — siehe TODO im Modul-Docstring"
        )
        yield  # type: ignore[unreachable]

    async def list_entities(
        self,
        body: dict,
        entity_type: EntityType,
        modified_since: datetime | None = None,  # noqa: ARG002 — RIS hat kein incremental
    ) -> AsyncIterator[dict]:
        """TODO: Per ``entity_type`` an spezifischen Sub-Scraper dispatchen.

        Empfohlene Modulstruktur (für später):
            ingestor/adapters/sessionnet/
                __init__.py
                _http.py        — geteilter Client + Throttle
                _parsers.py     — selectolax-Selektoren pro Page-Type
                meetings.py     — sitzungslang.asp Iteration + info.asp Detail
                papers.py       — Drucksachen-Liste + vorgang.asp
                persons.py      — personenuebersicht.asp + personendetail.asp
                organizations.py
                files.py
        """
        raise NotImplementedError(
            f"SessionNetAdapter.list_entities({entity_type.value}) — TODO"
        )
        yield  # type: ignore[unreachable]
