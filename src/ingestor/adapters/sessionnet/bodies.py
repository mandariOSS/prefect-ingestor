"""SessionNet body discovery.

SessionNet hat 1 Body pro Mandant. Der Mandant ist identisch mit der Source.
Wir synthetisieren den Body aus der Config (``base_url``) und scrapen den
Title der Landing-Page (``info.php``) für den ``name``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

import httpx

from ingestor.adapters.sessionnet._html import fetch_html, first_text, resolve

logger = logging.getLogger(__name__)


def synthesize_body_id(base_url: str) -> str:
    """Stabiler synthetischer ``id`` (URL) für den einzigen Body.

    Format: ``{base_url}#body``. Idempotent — gleiche base_url ergibt
    gleiche id, also stabil über Sync-Läufe.
    """
    base = base_url.rstrip("/")
    return f"{base}#body"


def construct_endpoints(base_url: str) -> dict[str, str]:
    """Mappt OParl-Endpoint-Felder auf SessionNet-PHP-URLs.

    Modernes SessionNet (V:050500+) nutzt ``.php``, älteres ``.asp``. Wir
    probieren ``.php`` zuerst (häufiger seit 2022), Fallback ``.asp``-Variante
    kann ein Sub-Adapter später bauen, wenn relevant.

    Diese URLs sind *Sammel-Listen-URLs* — der Sync ruft sie nicht direkt ab,
    sondern reicht sie an die jeweiligen Sub-Parser (meetings.list, etc.)
    weiter, die wiederum konkrete Filter/Pagination machen.
    """
    return {
        "meeting": resolve(base_url, "si0040.php"),
        "paper": resolve(base_url, "vo0040.php"),
        "person": resolve(base_url, "kp0041.php"),
        "organization": resolve(base_url, "gr0040.php"),
        # SessionNet hat keine separate Endpoints für Membership / AgendaItem /
        # Consultation / File / Location — die werden alle in den Detail-Views
        # der Meetings/Papers embedded extrahiert. Wir setzen None damit der
        # Sync-Flow sie entsprechend skipped.
        "membership": None,
        "agendaItem": None,
        "consultation": None,
        "file": None,
        "location": None,
        "legislativeTerm": None,
    }


async def discover(client: httpx.AsyncClient, base_url: str, config: dict) -> AsyncIterator[dict]:
    """Yieldet genau einen synthetisierten Body.

    Args:
        client: shared httpx client
        base_url: aus ``source.config["base_url"]``
        config: kompletter ``source.config`` (für Override-Werte wie
                ``short_name`` oder ``name``)
    """
    body_id = synthesize_body_id(base_url)
    endpoints = construct_endpoints(base_url)

    # Name aus Landing-Page-Title scrapen, sofern nicht in Config überschrieben
    name = config.get("name")
    short_name = config.get("short_name") or config.get("shortName")

    if not name:
        try:
            tree = await fetch_html(client, resolve(base_url, "info.php"))
            page_title = first_text(tree, "title") or "SessionNet"
            # Format meist: "SessionNet | Ratsinformationssystem"
            # → split nach " | " und letzten Teil nehmen, sonst Titel ganz
            if " | " in page_title:
                name = page_title.split(" | ", 1)[1].strip()
            else:
                name = page_title
        except Exception as exc:  # noqa: BLE001 — Fallback hat sinnvollen Default
            logger.debug("Body-name scrape failed (%s) — using fallback", exc)
            name = "SessionNet RIS"

    if not short_name:
        # Fallback: aus base_url-Hostname ableiten
        from urllib.parse import urlparse

        host = urlparse(base_url).hostname or "ris"
        short_name = host.split(".")[1] if host.startswith("www.") else host.split(".")[0]
        short_name = short_name.upper()[:10]

    yield {
        "id": body_id,
        "type": "https://schema.oparl.org/1.1/Body",
        "name": name,
        "shortName": short_name,
        # Pflicht-Endpoint-Felder gemäß OParl-Spec — None wo wir nicht liefern.
        # Der Sync-Flow toleriert None (skip), die OParl-Output-API wird sie
        # wegfiltern.
        **endpoints,
        # Mandari-Vendor-Extension: woher kommt der Body?
        "mandari:adapter": "ris_sessionnet",
        "mandari:baseUrl": base_url,
    }
