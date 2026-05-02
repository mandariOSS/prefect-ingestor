"""
SDNetAdapter — HTML-Scraper für Sternberg SD.NET RIM.

Vendor-Übersicht:
    Sternberg Software GmbH (Bielefeld)
    https://www.sitzungsdienst.net/
    https://www.sitzungsdienst.net/produkte/gremieninformationssystem/

    Größter Marktanteil nach Installationen (vendor-claim: 900+ Installations,
    31 000+ Users DACH-weit). Stark in NRW (KDVZ Frechen, KDVZ Rhein-Erft-Rur)
    und Hessen (ekom21 Setup nutzt SD.NET intern).

OParl-Status:
    Sternberg hat seit 2017 ein OParl-Modul (https://www.sitzungsdienst.net/
    produkte/gremieninformationssystem/oparl-schnittstelle/) und nach Stand
    open.NRW betreiben mindestens 21 Kommunen via KDVZ Frechen die OParl-
    Schnittstelle aktiv (sdnetrim.kdvz-frechen.de). Die meisten Sternberg-
    Installationen außerhalb dieses Verbunds haben aber kein aktives OParl.

URL-Pattern (Erkennungsmuster):
    https://{host}/sdnet/                                    → Landing
    https://{host}/sdnet/sit/sit01.asp                       → Sitzungs-Übersicht
    https://{host}/sdnet/sit/sit10.asp?GRA=...               → Gremien-Filter
    https://{host}/sdnet/sit/sit10.asp?id=...                → Sitzungs-Detail
    https://{host}/sdnet/vor/vor01.asp                       → Vorlagen-Liste
    https://{host}/sdnet/vor/vor10.asp?id=...                → Vorlage-Detail
    https://{host}/sdnet/per/per01.asp                       → Personen-Liste
    https://{host}/sdnet/per/per10.asp?id=...                → Person-Detail
    https://{host}/sdnet/gre/gre01.asp                       → Gremien-Liste
    https://{host}/sdnet/gre/gre10.asp?id=...                → Gremium-Detail

    Sternberg hat in den letzten Jahren die Anti-CSRF/SQLi-Hardening verstärkt
    (siehe https://www.kommune21.de/meldung_28242_Ratsinformationssystem+ist+sicher.html).
    Das macht Scraping marginal schwerer als bei SessionNet/ALLRIS:
        - Token-basierte Form-Submits auf manchen Filtern (CSRF)
        - User-Agent-Filter (manche Setups blocken non-Browser UAs aggressiv)
        - Einige Endpoints verlangen Referer-Header

    Aber: alle Pflicht-Daten sind weiterhin per simplen GET-Requests
    erreichbar, solange wir uns an Browser-ähnliche Headers halten.

Mapping-Referenz:
    KEIN politik-bei-uns-Vorgänger existiert für SD.NET (das war v.a.
    SessionNet/ALLRIS-fokussiert). Beste verfügbare Referenzen:
        1. Live-OParl-Output von kdvz-frechen Kommunen — die OParl-Antworten
           geben uns das Mapping ground-truth, weil Sternberg's eigenes
           OParl-Modul direkt aus den SD.NET-Tabellen serialisiert.
        2. Eigene Reverse-Engineering-Notes anlegen unter docs/adapters/sdnet.md

    Empfohlene Vorgehensweise: PARALLEL einen kdvz-frechen Mandanten via
    OParl-Adapter syncen + denselben via Scraper. Diff-Analyse zeigt, wo
    HTML-Werte mappen.

Source-Configuration:
    {
        "base_url": "https://ris.example.de/sdnet",
        "encoding": "utf-8",
        "use_referer_header": true,    # default; manche Setups blocken sonst
        "browser_emulation": "firefox" # default; setzt User-Agent + Accept
                                       # auf Firefox-defaults
    }

ENTWICKLUNGS-STATUS: SKELETON.

Implementierungsreihenfolge (empfohlen, weicht von SessionNet/ALLRIS ab):
    1. discover_bodies — wieder 1 Body/Source aus Config
    2. ORGANIZATION (gre01.asp) — wir brauchen Gremien-IDs für Sitzungs-Filter
    3. PERSON (per01.asp) — für Membership-Resolution später nötig
    4. MEMBERSHIP (gre10.asp Mitglieder-Block + Cross-Check mit per01)
    5. MEETING (sit10.asp pro Gremium iterieren)
    6. AGENDA_ITEM (in sit10.asp embedded)
    7. PAPER (vor01.asp)
    8. CONSULTATION (Cross-Reference Paper×Meeting aus sit10 TOPs)
    9. FILE (PDF-Anhänge)
    10. LOCATION (Sitzungsraum aus sit10, optional)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime

from ingestor.adapters._scraper_base import ScraperAdapterBase
from ingestor.adapters.base import EntityType

logger = logging.getLogger(__name__)


class SDNetAdapter(ScraperAdapterBase):
    """Scraper für Sternberg SD.NET RIM. SKELETON."""

    ADAPTER_TYPE = "ris_sdnet"

    #: Sternberg ist konservativ — 2 s zwischen Requests ist defensiv aber
    #: respektvoll. Sollte auch durch eventuelle WAF-Filter durchgehen.
    REQUEST_INTERVAL_SECONDS = 2.0

    async def discover_bodies(self) -> AsyncIterator[dict]:
        """TODO: Body aus Config synthetisieren.

        SD.NET-Endpoints landen auf sit/, vor/, per/, gre/-Pfaden.
        """
        raise NotImplementedError(
            "SDNetAdapter.discover_bodies — siehe TODO im Modul-Docstring"
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
            ingestor/adapters/sdnet/
                meetings.py     — sit/sit01.asp + sit10.asp
                papers.py       — vor/vor01.asp
                persons.py      — per/per01.asp
                organizations.py — gre/gre01.asp + gre10.asp
                files.py
        """
        raise NotImplementedError(
            f"SDNetAdapter.list_entities({entity_type.value}) — TODO"
        )
        yield  # type: ignore[unreachable]
