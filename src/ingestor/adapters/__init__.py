"""
Source-Adapter-Framework — eine Abstraktionsschicht über jedem RIS-Backend.

Mandari Ingestor unterstützt mehrere Datenquellen-Typen:

    1. **OParl** (default) — direkter HTTP-Konsum von OParl 1.0/1.1 APIs.
       Adapter-Type: ``"oparl"``. Funktioniert für ~1.4 % der deutschen
       Kommunen (147 von ~10 800), wo der RIS-Hersteller die OParl-
       Schnittstelle aktiviert hat.

    2. **Somacos SessionNet / Mandatos** — größte Vendor-Familie (~2 000
       Installationen, AKDB/Bayern dominant). Adapter-Type:
       ``"ris_sessionnet"``. Klassisches ASP, stabile URL-Patterns
       (``/bi/...asp``).

    3. **CC e-gov ALLRIS** — NRW + Berlin-Bezirke. Adapter-Type:
       ``"ris_allris"``. Server-rendered HTML, konsistentes Layout.

    4. **Sternberg SD.NET RIM** — 900+ Installationen DACH-weit, oft mit
       partieller OParl-Unterstützung. Adapter-Type: ``"ris_sdnet"``.

Architektur:

    Source.adapter_type   ──►   ADAPTERS-Registry   ──►   BaseAdapter-Subclass
                                                                │
                                                                ▼
                                                    discover_bodies()
                                                    list_entities(body, type, since)
                                                                │
                                                                ▼
                                                       OParl-shape dicts
                                                                │
                                                                ▼
                                                      upsert_entity() (unverändert)

Output-Kontrakt: Jeder Adapter MUSS OParl-1.1-shape-JSON-dicts liefern, damit
der downstream-Pipeline (upsert_entity, OParl-API-Reconstruction) keine
Vendor-Specials kennen muss. Vendor-spezifische Felder dürfen unter
``mandari:*`` Vendor-Extensions hinzugefügt werden.

Entwicklungs-Status (Stand v0.2):
    - ``oparl``        : production-ready
    - ``ris_sessionnet``: SKELETON (TODO)
    - ``ris_allris``   : SKELETON (TODO)
    - ``ris_sdnet``    : SKELETON (TODO)
"""

from ingestor.adapters.base import (
    AdapterError,
    AdapterNotFoundError,
    BaseAdapter,
    EntityType,
)
from ingestor.adapters.oparl_adapter import OParlAdapter
from ingestor.adapters.registry import ADAPTERS, get_adapter_class
from ingestor.adapters.ris_allris import AllRISAdapter
from ingestor.adapters.ris_sdnet import SDNetAdapter
from ingestor.adapters.ris_sessionnet import SessionNetAdapter

__all__ = [
    "ADAPTERS",
    "AdapterError",
    "AdapterNotFoundError",
    "AllRISAdapter",
    "BaseAdapter",
    "EntityType",
    "OParlAdapter",
    "SDNetAdapter",
    "SessionNetAdapter",
    "get_adapter_class",
]
