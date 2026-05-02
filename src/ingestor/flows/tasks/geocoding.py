"""
Geocoding-Tasks via Nominatim (OpenStreetMap).

Architektur (seit v0.2):
    1. Adressen werden während des Sync-Flows NICHT mehr direkt geocodet.
    2. Stattdessen schreibt der Sync nur die Adress-Spalten (street_address,
       locality) in die Location-Tabelle.
    3. Der dedizierte GeocodingWorker (siehe workers/geocoding.py) pickt
       fehlende Geo-Koordinaten Stück für Stück ab — strikt seriell, mit
       konfigurierbarem Sleep zwischen Requests, damit die Public-Nominatim-
       Usage-Policy (≤ 1 Request pro Sekunde) sicher eingehalten wird.

    4. Diese Datei stellt nur die *Low-Level-Funktion* `geocode_address()`
       bereit (eine einzelne HTTP-Anfrage), die vom Worker im Loop benutzt
       wird. Kein paralleles Geocoding mehr im Flow.

Public Nominatim Usage Policy (Stand 2026):
    https://operations.osmfoundation.org/policies/nominatim/
    - Maximal 1 Anfrage pro Sekunde
    - Pflicht-Header: User-Agent mit identifizierbarer App + Kontakt
    - Caching der Ergebnisse zwingend
    - Bei größerem Volumen: eigene Instanz aufsetzen

Eigenbetrieb (optional, wenn Volumen groß wird):
    Lokale Nominatim-Instanz nach Anleitung
    https://nominatim.org/release-docs/latest/admin/Installation/
    Mandari-Stack hat sie BEWUSST nicht im docker-compose, weil ~10 GB DB +
    2-4 h Initial-Import die meisten Self-Hoster überfordert. Wer sie braucht,
    setzt NOMINATIM_URL auf die eigene Instanz und kann den Worker auf
    GEOCODING_INTERVAL_SECONDS=0.1 oder ähnlich beschleunigen.
"""

from __future__ import annotations

import logging

import httpx

from ingestor.config import get_settings

logger = logging.getLogger(__name__)


async def geocode_address(
    address: str,
    locality: str | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Löst eine einzelne Adresse in Koordinaten auf.

    Args:
        address: Straße + Hausnummer, z.B. "Musterstraße 42"
        locality: Optional Ort/Stadt, wird angehängt für bessere Treffer
        client: Optional vorhandener AsyncClient (Worker reused den Client
                über alle Iterationen — spart TCP-Handshakes)

    Returns:
        ``{"lat": float, "lon": float, "display_name": str}`` bei Treffer,
        ``None`` bei keinem Treffer oder Fehler.

    Wirft KEINE Exceptions — alle Fehler werden geloggt und None zurückgegeben.
    Der Worker entscheidet, was bei None passiert (skip, retry-counter ++, …).
    """
    settings = get_settings()
    nominatim_url = settings.nominatim_url.rstrip("/")
    if not nominatim_url:
        logger.debug("NOMINATIM_URL leer — geocoding deaktiviert")
        return None

    query = address
    if locality:
        query = f"{address}, {locality}, Deutschland"

    headers = {
        # Pflicht laut Nominatim Usage Policy
        "User-Agent": settings.nominatim_user_agent,
        "Accept": "application/json",
        "Accept-Language": "de",
    }
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "de",
        "addressdetails": 0,
    }
    if settings.nominatim_email:
        params["email"] = settings.nominatim_email

    try:
        if client is None:
            async with httpx.AsyncClient(timeout=15.0, headers=headers) as ad_hoc:
                response = await ad_hoc.get(f"{nominatim_url}/search", params=params)
        else:
            response = await client.get(
                f"{nominatim_url}/search",
                params=params,
                headers=headers,
            )

        if response.status_code == 429:
            logger.warning(
                "Nominatim rate-limited (HTTP 429) — Worker-Intervall vergrößern"
            )
            return None
        if response.status_code != 200:
            logger.debug(
                "Nominatim HTTP %s for '%s'", response.status_code, query
            )
            return None

        results = response.json()
        if not results:
            return None

        return {
            "lat": float(results[0]["lat"]),
            "lon": float(results[0]["lon"]),
            "display_name": results[0].get("display_name"),
        }
    except (httpx.HTTPError, KeyError, ValueError) as exc:
        logger.debug("Geocode failed for '%s': %s", query, exc)
        return None
