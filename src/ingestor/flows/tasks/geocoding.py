"""
Task: Geo-Anreicherung von Meetings/Locations via Nominatim.

Nutzt eine lokale Nominatim-Instanz (Docker, nur DE-Daten ~3GB)
um Adressen in Koordinaten aufzulösen.

Docker-Compose für lokale Nominatim:
    nominatim:
        image: mediagis/nominatim:4.5
        container_name: ingestor-nominatim
        environment:
            PBF_URL: https://download.geofabrik.de/europe/germany-latest.osm.pbf
            REPLICATION_URL: https://download.geofabrik.de/europe/germany-updates/
            NOMINATIM_PASSWORD: nominatim
        volumes:
            - nominatim_data:/var/lib/postgresql/14/main
        ports:
            - "8088:8080"

~3GB Download, ~10GB DB, initialer Import ~2-4h.
Danach: Unbegrenzte Anfragen, keine Rate-Limits.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

import httpx
from prefect import flow, get_run_logger, task
from sqlalchemy import and_, select

from ingestor.db import get_session
from ingestor.db.models import Location

logger = logging.getLogger(__name__)

# Lokale Nominatim-URL (Docker)
NOMINATIM_URL = "http://localhost:8088"


@task(name="geocode-address", retries=2, retry_delay_seconds=2)
async def geocode_address(address: str, locality: str | None = None) -> dict | None:
    """
    Löst eine Adresse in Koordinaten auf via lokaler Nominatim.

    Returns: {"lat": float, "lon": float} oder None.
    """
    query = address
    if locality:
        query = f"{address}, {locality}, Deutschland"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{NOMINATIM_URL}/search",
                params={
                    "q": query,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "de",
                },
            )
            if response.status_code != 200:
                return None

            results = response.json()
            if not results:
                return None

            return {
                "lat": float(results[0]["lat"]),
                "lon": float(results[0]["lon"]),
                "display_name": results[0].get("display_name"),
            }
    except Exception as exc:
        logger.debug("Geocode failed for '%s': %s", query, exc)
        return None


@flow(name="geocode-locations", log_prints=True)
async def geocode_locations(body_id: UUID, max_concurrent: int = 3) -> dict:
    """
    Löst alle Locations eines Bodies ohne Koordinaten auf.
    """
    log = get_run_logger()

    async with get_session() as session:
        locations = (
            (
                await session.execute(
                    select(Location).where(
                        and_(
                            Location.body_id == body_id,
                            Location.latitude.is_(None),
                            Location.street_address.is_not(None),
                        )
                    )
                )
            )
            .scalars()
            .all()
        )

    log.info("Geocoding %d locations for body %s", len(locations), body_id)

    semaphore = asyncio.Semaphore(max_concurrent)
    success = 0

    async def process_location(loc: Location) -> bool:
        async with semaphore:
            result = await geocode_address(
                loc.street_address or loc.description or "",
                locality=loc.locality,
            )
            if not result:
                return False

            async with get_session() as session:
                db_loc = (await session.execute(select(Location).where(Location.id == loc.id))).scalar_one()
                db_loc.latitude = result["lat"]
                db_loc.longitude = result["lon"]
                await session.flush()
            return True

    results = await asyncio.gather(*[process_location(loc) for loc in locations], return_exceptions=True)
    success = sum(1 for r in results if r is True)

    log.info("Geocoded %d/%d locations", success, len(locations))
    return {"tried": len(locations), "success": success}
