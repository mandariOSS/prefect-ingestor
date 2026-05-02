"""
AllRISAdapter — HTML-Scraper für CC e-gov ALLRIS / ALLRIS net.

Vendor-Übersicht:
    CC e-gov GmbH (Hamburg)
    https://www.cc-egov.de/allris-4/
    https://www.cc-egov.de/

    Zweiter großer Player im deutschen RIS-Markt. Stark in NRW und in den
    Berliner Bezirksämtern (12 von 12 Bezirke nutzen ALLRIS-Varianten).
    Drei Produktlinien:
        - ALLRIS net      — Web-RIS (das was wir scrapen)
        - ALLRIS kompakt  — kleinere Kommunen
        - ALLRIS 4        — moderne Variante (geringere Verbreitung Stand 2026)

OParl-Status:
    OParl-Modul existiert und ist relativ häufig aktiviert (~30 % geschätzt
    in NRW). Wo aktiv → OParlAdapter benutzen. Dieser Scraper für die
    restlichen ~70 %.

URL-Pattern (Erkennungsmuster):
    https://{host}/bi/                                  → Übersicht
    https://{host}/bi/si010_e.asp                       → Sitzungs-Suchformular
    https://{host}/bi/to010.asp?SILFDNR=...             → Tagesordnung einer Sitzung
    https://{host}/bi/vo020.asp?VOLFDNR=...             → Vorlage-Detail
    https://{host}/bi/kp020.asp?KPLFDNR=...             → Mandatsträger-Detail
    https://{host}/bi/au020.asp?AULFDNR=...             → Gremium-Detail

    Sehr ähnlich zu SessionNet (beide sind ASP-basiert, ähnliche Konventionen),
    aber abweichende Parameter-Namen (SILFDNR vs __ksinr).
    Stable URL-Patterns seit ca. 2010.

Mapping-Referenz:
    Wieder politik-bei-uns/scrape-a-ris als de-facto-Doku, hier der
    "allris"-Sub-Folder. Plus OpenRuhr/ris-scraper hat einen ALLRIS-spezifischen
    Parser, etwas neuer (2018) als der politik-bei-uns Stand.

    github.com/OpenRuhr/ris-scraper/tree/master/scraper/allris

Source-Configuration:
    {
        "base_url": "https://ratsinfo.example.de/bi",
        "encoding": "utf-8",       # default; alte Instanzen iso-8859-1
        "subscriber_id": "1"       # ALLRIS-Mandant-Identifier
    }

ENTWICKLUNGS-STATUS: SKELETON.

Implementierungsreihenfolge:
    1. discover_bodies — wieder 1 Body/Source aus Config
    2. MEETING (si010_e.asp Suche, dann to010.asp pro Sitzung)
    3. AGENDA_ITEM (in to010.asp embedded)
    4. PAPER (vo020.asp)
    5. PERSON (kp020.asp + Listen-Scraper)
    6. ORGANIZATION (au020.asp)
    7. MEMBERSHIP (au020.asp Mitglieder-Block)
    8. FILE (PDF-Anhänge in Sitzungs- und Vorlagen-Details)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime

from ingestor.adapters._scraper_base import ScraperAdapterBase
from ingestor.adapters.base import EntityType

logger = logging.getLogger(__name__)


class AllRISAdapter(ScraperAdapterBase):
    """Scraper für CC e-gov ALLRIS net. SKELETON."""

    ADAPTER_TYPE = "ris_allris"

    REQUEST_INTERVAL_SECONDS = 1.5

    async def discover_bodies(self) -> AsyncIterator[dict]:
        """TODO: Body aus ``source.config["base_url"]`` synthetisieren.

        Body-Felder analog SessionNet — die Endpoint-URLs zeigen aber auf
        ALLRIS-Pfade (si010_e.asp etc.).
        """
        raise NotImplementedError(
            "AllRISAdapter.discover_bodies — siehe TODO im Modul-Docstring"
        )
        yield  # type: ignore[unreachable]

    async def list_entities(
        self,
        body: dict,
        entity_type: EntityType,
        modified_since: datetime | None = None,  # noqa: ARG002
    ) -> AsyncIterator[dict]:
        """TODO: Per entity_type an Sub-Scraper dispatchen.

        Empfohlene Modulstruktur (für später):
            ingestor/adapters/allris/
                meetings.py     — si010_e.asp + to010.asp
                papers.py       — vo020.asp
                persons.py      — kp020.asp
                organizations.py — au020.asp
                files.py
        """
        raise NotImplementedError(
            f"AllRISAdapter.list_entities({entity_type.value}) — TODO"
        )
        yield  # type: ignore[unreachable]
