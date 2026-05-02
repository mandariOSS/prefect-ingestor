"""
BaseAdapter — gemeinsamer Vertrag aller Datenquellen-Adapter.

Jeder konkrete Adapter (OParl, SessionNet, ALLRIS, SD.NET, ...) implementiert
diese ABC. Die Methoden sind so geschnitten, dass die downstream-Pipeline
(``flows/sync_source.py``, ``flows/tasks/upsert.py``, OParl-API-Reconstruction)
keine Vendor-Spezifika sehen muss — sie erwartet einfach OParl-shape dicts.

Wichtig:
    - Alle ``list_*``-Methoden sind ASYNC ITERATORS (``AsyncIterator[dict]``),
      damit der Sync-Flow streaming pagination machen kann ohne Memory-Blow-up.
    - Adapter sind ASYNC CONTEXT MANAGERS — ``async with adapter as a:`` öffnet
      und schließt geteilte HTTP-Clients.
    - Adapter dürfen den Source-Eintrag lesen, aber nicht schreiben. Datenbank-
      Persistenz macht ausschließlich der Sync-Flow.
"""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ingestor.db.models import Source


class EntityType(str, enum.Enum):
    """OParl-Entity-Typen, die ein Adapter liefern kann.

    Die Werte matchen die OParl-Endpoint-Feldnamen aus Body
    (z.B. ``body["meeting"]`` enthält die URL für meetings).
    Ein Adapter darf einen Subset implementieren — nicht jedes RIS hat alle
    Konzepte (z.B. ``legislativeTerm`` fehlt in vielen Scrapings).
    """

    LEGISLATIVE_TERM = "legislativeTerm"
    ORGANIZATION = "organization"
    PERSON = "person"
    MEMBERSHIP = "membership"
    MEETING = "meeting"
    PAPER = "paper"
    AGENDA_ITEM = "agendaItem"
    CONSULTATION = "consultation"
    FILE = "file"
    LOCATION = "location"


class AdapterError(Exception):
    """Allgemeiner Adapter-Fehler."""


class AdapterNotFoundError(AdapterError):
    """Kein Adapter für den angegebenen ``adapter_type`` registriert."""


class AdapterFetchError(AdapterError):
    """Fehler beim Abruf einer einzelnen Resource."""


class BaseAdapter(ABC):
    """Adapter-Vertrag.

    Subclasses implementieren die ``_async_*``-Hooks (oder die public Methoden
    direkt) — siehe ``OParlAdapter`` für die Referenzimplementierung.

    Lifecycle:

        async with AdapterCls(source) as adapter:
            async for body in adapter.discover_bodies():
                async for meeting in adapter.list_entities(body, EntityType.MEETING):
                    ...
    """

    #: Eindeutiger Adapter-Type-Identifier, MUSS mit dem ``adapter_type`` in
    #: der ``sources``-Tabelle übereinstimmen. Wird auch für Logging und
    #: Telemetrie verwendet.
    ADAPTER_TYPE: str = "base"

    def __init__(self, source: Source) -> None:
        self.source = source

    # ─────────────────────── Lifecycle ───────────────────────

    @abstractmethod
    async def __aenter__(self) -> Self:
        """Öffnet HTTP-Client / Browser-Session / etc."""

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Schließt alle offenen Ressourcen."""

    # ─────────────────────── Discovery ───────────────────────

    @abstractmethod
    def discover_bodies(self) -> AsyncIterator[dict]:
        """Liefert OParl-shape Body-Dicts.

        Für OParl-Quellen: lädt das ``system``-Endpoint und iteriert über
        ``system["body"]``.

        Für RIS-Scraper: liest die Source-Konfiguration (``source.config``),
        scrapt die Mandanten-Liste oder nutzt eine in der Config hinterlegte
        Body-Definition. Mehrere Bodies pro Source sind möglich (z.B. ein
        Stadt-RIS mit mehreren Bezirks-Mandanten).

        Yields:
            OParl-Body-Dicts mit Pflichtfeldern:
              - ``id`` (URL oder synthetische URL)
              - ``type`` = "https://schema.oparl.org/1.1/Body"
              - ``shortName``, ``name``
              - Endpoint-URLs: ``meeting``, ``paper``, ``person``, ``organization``,
                ``membership``, ``agendaItem``, ``consultation``, ``file``, ``location``
                (oder None, falls der Adapter sie nicht liefert)
        """
        # ABSTRACT — must be implemented as async generator in subclass.
        # Annotation only, never called via super().
        raise NotImplementedError
        yield  # type: ignore[unreachable]  # makes it a generator at typecheck time

    # ─────────────────────── Listing ───────────────────────

    @abstractmethod
    def list_entities(
        self,
        body: dict,
        entity_type: EntityType,
        modified_since: datetime | None = None,
    ) -> AsyncIterator[dict]:
        """Liefert OParl-shape Entity-Dicts eines Typs für einen Body.

        Args:
            body: Body-Dict (aus ``discover_bodies``)
            entity_type: ``EntityType.MEETING``, ``EntityType.PAPER`` etc.
            modified_since: Nur Entities ab diesem Zeitpunkt (für Incremental-Sync).
                Adapter, die kein Incremental unterstützen (typisch für
                Scraper), dürfen den Parameter ignorieren — der Sync-Flow
                handelt dann full-sync-Verhalten ab.

        Yields:
            OParl-shape Entity-Dicts. Pflichtfelder pro Entity-Typ siehe
            https://oparl.org/spec/

        Raises:
            AdapterFetchError: Bei nicht-recoverable HTTP-/Parse-Fehlern.
            Recoverable Fehler (Timeouts, einzelne 500er) sollten innerhalb
            des Adapters retried werden, bevor diese Exception fliegt.
        """
        raise NotImplementedError
        yield  # type: ignore[unreachable]

    # ─────────────────────── Optional helpers ───────────────────────

    async def get_entity(self, url: str) -> dict | None:
        """Lädt eine einzelne Entity per URL/ID nach.

        Default-Implementation: ``None`` zurückgeben — Adapter, die das
        unterstützen wollen (z.B. OParl), überschreiben sie. Wird vom
        Sync-Flow für embedded-Object-Resolution genutzt.
        """
        return None

    def supports_incremental(self) -> bool:
        """Ob ``modified_since`` echten Effekt hat.

        Default: False. OParlAdapter überschreibt zu True. Scraper-Adapter
        können meistens nur full-sync, weil kein RIS-HTML einen "geändert
        seit"-Filter exposed.
        """
        return False
