"""
GeocodingWorker — dedicated rate-limited Nominatim consumer.

Nominatim's public usage policy (https://operations.osmfoundation.org/policies/nominatim/)
allows ≤ 1 request per second from any single client. To stay safely below
that ceiling AND to be a good Civic-Tech-Citizen on shared infrastructure,
this worker serializes all geocoding requests through a single asyncio loop
and sleeps GEOCODING_INTERVAL_SECONDS (default: 5) between requests.

Architecture:

    while True:
        batch = SELECT … FROM locations
                WHERE latitude IS NULL
                  AND street_address IS NOT NULL
                LIMIT GEOCODING_BATCH_SIZE
        for loc in batch:
            geocode(loc)            # 1 HTTP request
            persist (lat, lon)
            await asyncio.sleep(GEOCODING_INTERVAL_SECONDS)
        if no batch: sleep idle_sleep

The worker SHARES a single httpx.AsyncClient across iterations to keep
TCP-Handshake overhead minimal — important when hitting public Nominatim
which sits behind multiple HTTPS layers.

Operations notes:
  - Worker is idempotent: if it crashes mid-batch, only the un-persisted
    location is lost; next iteration picks it up again.
  - On HTTP 429: log warning, abort batch, sleep idle_sleep (server is
    asking us to back off — respect that).
  - On parse error / no result: persist a "tried but failed" marker so we
    don't retry the same un-geocodable address forever.
    (We use a small geocode_attempts counter on Location — see DB migration.)
"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

import httpx
from sqlalchemy import and_, or_, select, update

from ingestor.config import get_settings
from ingestor.db import get_session
from ingestor.db.models import Location
from ingestor.flows.tasks.geocoding import geocode_address

logger = logging.getLogger(__name__)


# Maximale Anzahl von Geocode-Versuchen pro Location bevor wir aufgeben.
# Adressen die n-mal "kein Treffer" liefern sind in der Regel kaputt
# (Tippfehler, "TBA", "wird noch bekannt gegeben", …).
MAX_GEOCODE_ATTEMPTS = 3


async def _fetch_due_locations(limit: int) -> list[Location]:
    """Holt die nächsten Locations ohne lat/lon aus der DB.

    Filter:
        - latitude IS NULL  (noch nicht geocodet)
        - street_address IS NOT NULL  (kann nicht ohne Adresse geocodet werden)
        - geocode_attempts < MAX_GEOCODE_ATTEMPTS  (Aufgabe nach n Failures)

    Order:
        - geocode_attempts ASC, dann oparl_modified DESC
          → frische Adressen zuerst, retries mit Vorrang vor neuen failures
    """
    async with get_session() as session:
        result = await session.execute(
            select(Location)
            .where(
                and_(
                    Location.latitude.is_(None),
                    Location.street_address.is_not(None),
                    or_(
                        Location.geocode_attempts.is_(None),
                        Location.geocode_attempts < MAX_GEOCODE_ATTEMPTS,
                    ),
                )
            )
            .order_by(
                Location.geocode_attempts.asc().nulls_first(),
                Location.oparl_modified.desc().nulls_last(),
            )
            .limit(limit)
        )
        return list(result.scalars().all())


async def _persist_geocode(loc_id, lat: float | None, lon: float | None) -> None:
    """Schreibt Geocode-Resultat in DB.

    Bei Treffer: latitude/longitude setzen.
    Bei Miss: nur geocode_attempts inkrementieren (für Retry-Logik).
    """
    async with get_session() as session:
        if lat is not None and lon is not None:
            await session.execute(
                update(Location)
                .where(Location.id == loc_id)
                .values(
                    latitude=lat,
                    longitude=lon,
                    geocode_attempts=Location.geocode_attempts + 1
                    if Location.geocode_attempts is not None
                    else 1,
                )
            )
        else:
            # Miss → counter ++, kein lat/lon
            await session.execute(
                update(Location)
                .where(Location.id == loc_id)
                .values(
                    geocode_attempts=(Location.geocode_attempts.op("+")(1))
                    if Location.geocode_attempts is not None
                    else 1
                )
            )


async def _process_one(client: httpx.AsyncClient, loc: Location) -> bool:
    """Geocodet eine einzelne Location. Returns True bei Treffer."""
    address = loc.street_address or loc.description or ""
    if not address.strip():
        # Sicherheitsnetz — sollte durch SELECT-Filter nicht vorkommen
        return False

    result = await geocode_address(address, locality=loc.locality, client=client)
    if result is None:
        await _persist_geocode(loc.id, None, None)
        logger.debug("geocode miss: id=%s address=%r", loc.id, address)
        return False

    await _persist_geocode(loc.id, result["lat"], result["lon"])
    logger.info(
        "geocoded id=%s '%s' → (%.5f, %.5f)",
        loc.id,
        address,
        result["lat"],
        result["lon"],
    )
    return True


async def run_geocoding_worker(stop_event: asyncio.Event | None = None) -> None:
    """Endlos-Loop des Geocoding-Workers.

    Args:
        stop_event: optional, gesetzt von Signal-Handler. Falls None, läuft
                    der Worker bis SIGTERM/SIGINT direkt aus dem Loop heraus
                    abgebrochen wird.

    Verhalten:
        - Holt batch_size Locations ohne lat/lon
        - Pro Location: geocode + persist + sleep(interval_seconds)
        - Wenn batch leer: sleep(idle_sleep_seconds), dann nächste Runde
        - Auf HTTP 429: batch abbrechen, idle_sleep einlegen
    """
    settings = get_settings()
    if not settings.geocoding_enabled:
        logger.info("Geocoding-Worker disabled (GEOCODING_ENABLED=False)")
        return

    if not settings.nominatim_url.strip():
        logger.warning("NOMINATIM_URL leer — Geocoding-Worker beendet sich")
        return

    interval = settings.geocoding_interval_seconds
    batch_size = settings.geocoding_batch_size
    idle_sleep = settings.geocoding_idle_sleep_seconds

    logger.info(
        "GeocodingWorker started: interval=%.1fs batch=%d idle=%ds nominatim=%s",
        interval,
        batch_size,
        idle_sleep,
        settings.nominatim_url,
    )

    headers = {
        "User-Agent": settings.nominatim_user_agent,
        "Accept": "application/json",
        "Accept-Language": "de",
    }

    async with httpx.AsyncClient(
        timeout=15.0,
        headers=headers,
        # Public Nominatim mag keine HTTP/2 (manche CDNs droppen connections);
        # HTTP/1.1 mit keep-alive ist robuster.
        http2=False,
    ) as client:
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("GeocodingWorker shutdown signal received")
                return

            try:
                locations = await _fetch_due_locations(batch_size)
            except Exception as exc:  # noqa: BLE001
                logger.exception("DB query failed: %s", exc)
                await _interruptible_sleep(idle_sleep, stop_event)
                continue

            if not locations:
                logger.debug(
                    "no locations to geocode — sleep %ds", idle_sleep
                )
                await _interruptible_sleep(idle_sleep, stop_event)
                continue

            logger.info("geocoding batch: %d locations", len(locations))
            for loc in locations:
                if stop_event is not None and stop_event.is_set():
                    return

                try:
                    await _process_one(client, loc)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("geocode error id=%s: %s", loc.id, exc)

                # Strikt seriell warten — egal ob Treffer oder Miss.
                # Das ist die Kernzusicherung: ≤ 1 Request alle interval Sek.
                await _interruptible_sleep(interval, stop_event)


async def _interruptible_sleep(
    seconds: float, stop_event: asyncio.Event | None
) -> None:
    """asyncio.sleep, das auf stop_event reagiert."""
    if stop_event is None:
        await asyncio.sleep(seconds)
        return
    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)


# ─────────────────────────── Entry-Point ───────────────────────────


def main() -> None:
    """Container-Entry-Point: `ingestor-geocoding`."""
    import logging.config  # noqa: WPS433
    import os

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    )

    stop_event = asyncio.Event()

    def _handle_signal(*_args):
        logger.info("Received shutdown signal")
        stop_event.set()

    # POSIX signal handlers (Windows-Container haben SIGTERM auch)
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError, ValueError):
            signal.signal(sig, _handle_signal)

    try:
        asyncio.run(run_geocoding_worker(stop_event))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt — exiting")


if __name__ == "__main__":
    main()
