"""
Adapter-Registry — mapping ``adapter_type`` strings to BaseAdapter subclasses.

Wird vom Sync-Flow genutzt, um pro Source dynamisch den richtigen Adapter
zu instanziieren:

    adapter_cls = get_adapter_class(source.adapter_type)
    async with adapter_cls(source) as adapter:
        ...

Neue Adapter werden hier registriert. Es gibt absichtlich keinen Plugin-
Mechanismus mit setuptools-Entry-Points — Adapter sind Kern-Bestandteil
des Ingestors und sollen explizit gepflegt werden.
"""

from __future__ import annotations

from ingestor.adapters.base import AdapterNotFoundError, BaseAdapter
from ingestor.adapters.oparl_adapter import OParlAdapter
from ingestor.adapters.ris_allris import AllRISAdapter
from ingestor.adapters.ris_sdnet import SDNetAdapter
from ingestor.adapters.ris_sessionnet import SessionNetAdapter

#: Adapter-Type → Klasse. Die Keys MÜSSEN mit dem ``Source.adapter_type``-
#: Spalten-Wert in der DB matchen.
ADAPTERS: dict[str, type[BaseAdapter]] = {
    "oparl": OParlAdapter,
    "ris_sessionnet": SessionNetAdapter,
    "ris_allris": AllRISAdapter,
    "ris_sdnet": SDNetAdapter,
}


def get_adapter_class(adapter_type: str) -> type[BaseAdapter]:
    """Liefert die Adapter-Klasse für einen gegebenen Type-String.

    Raises:
        AdapterNotFoundError: Wenn ``adapter_type`` nicht registriert ist.
    """
    cls = ADAPTERS.get(adapter_type)
    if cls is None:
        registered = ", ".join(sorted(ADAPTERS.keys()))
        raise AdapterNotFoundError(
            f"Unknown adapter_type='{adapter_type}'. "
            f"Registered: {registered}"
        )
    return cls
