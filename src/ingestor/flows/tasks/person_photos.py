"""
Task: Personenbilder herunterladen.

OParl enthält KEINE Personenbilder. Die meisten RIS-Systeme haben
aber ein Foto-URL-Pattern, das sich aus der Person-ID ableiten lässt.

Bekannte Patterns:
- session.net / Allris: /bi/person/<id>/foto.jpg (z.B. Münster, Berlin)
- KDVZ-Frechen (sdnetrim): /rim<num>/webservice/oparl/v1.1/person/<id>/image
- ekom21 (rim): /person/<id>/image
- gremien.info: kein Bild-Support

Die Bilder werden als BYTEA in der DB gespeichert (klein, ~20-100KB pro Bild).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

import httpx
from prefect import flow, get_run_logger, task
from sqlalchemy import and_, select

from ingestor.db import get_session
from ingestor.db.models import Body, Person

logger = logging.getLogger(__name__)

# Bekannte RIS-Foto-URL-Patterns (basierend auf System-URL)
PHOTO_URL_PATTERNS: dict[str, str] = {
    # session.net / Allris: Person-URL + /foto.jpg
    "sitzung-online.de": "{person_url}/foto.jpg",
    "sitzungsdienst-": "{person_url}/foto.jpg",
    # sessionnet (Münster etc.)
    "sessionnet": "{base_url}/bi/person/{person_id}/foto.jpg",
    # KDVZ Frechen (sdnetrim)
    "sdnetrim.kdvz-frechen.de": "{person_url}/image",
    # ekom21 (rim)
    "rim.ekom21.de": "{person_url}/image",
    # ratsinfomanagement.net
    "ratsinfomanagement.net": "{person_url}/image",
    # Fallback: OParl external_id + /image
    "_default": "{person_url}/image",
}


def _build_photo_url(person_external_id: str, body_raw: dict) -> str | None:
    """Konstruiert die Foto-URL aus der Person-External-ID und dem Body-System."""
    if not person_external_id:
        return None

    # Einfaches Pattern: /image an die Person-URL anhängen
    # Das funktioniert bei den meisten modernen RIS-Systemen
    base = person_external_id.rstrip("/")

    # Für session.net/Allris: /foto.jpg statt /image
    system_url = body_raw.get("system", "")
    if "sitzung-online.de" in system_url or "sitzungsdienst" in system_url:
        return f"{base}/foto.jpg"

    return f"{base}/image"


@task(name="download-person-photo", retries=1, retry_delay_seconds=5)
async def download_person_photo(person_id: UUID, photo_url: str) -> bool:
    """Lädt ein einzelnes Personenbild herunter und speichert es in der DB."""
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            response = await client.get(photo_url, headers={"User-Agent": "Mandari-Ingestor/0.1"})

        if response.status_code != 200:
            return False

        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("image/"):
            return False

        data = response.content
        if len(data) < 500:  # Zu klein = Platzhalter/leer
            return False

        async with get_session() as session:
            person = (
                await session.execute(select(Person).where(Person.id == person_id))
            ).scalar_one_or_none()
            if not person:
                return False

            person.photo_data = data
            person.photo_mime_type = content_type
            person.photo_url = photo_url
            person.photo_downloaded_at = datetime.now(UTC)
            await session.flush()

        return True
    except Exception as exc:
        logger.debug("Photo download failed for %s: %s", photo_url, exc)
        return False


@flow(name="fetch-person-photos", log_prints=True)
async def fetch_person_photos(body_id: UUID, max_concurrent: int = 5) -> dict:
    """
    Lädt Personenbilder für alle Personen eines Bodies herunter.

    Nur Personen ohne Bild (`photo_downloaded_at IS NULL`) werden versucht.
    """
    log = get_run_logger()

    async with get_session() as session:
        body = (await session.execute(select(Body).where(Body.id == body_id))).scalar_one_or_none()
        if not body:
            return {"error": "Body nicht gefunden"}

        # Personen ohne Foto laden
        persons = (
            (
                await session.execute(
                    select(Person).where(
                        and_(Person.body_id == body_id, Person.photo_downloaded_at.is_(None))
                    )
                )
            )
            .scalars()
            .all()
        )

    log.info("Trying photos for %d persons in %s", len(persons), body.name)

    body_raw = body.raw or {}
    semaphore = asyncio.Semaphore(max_concurrent)
    success = 0

    async def try_photo(person: Person) -> bool:
        async with semaphore:
            url = _build_photo_url(person.external_id, body_raw)
            if not url:
                return False
            return await download_person_photo(person.id, url)

    results = await asyncio.gather(*[try_photo(p) for p in persons], return_exceptions=True)
    success = sum(1 for r in results if r is True)

    log.info("Downloaded %d/%d photos for %s", success, len(persons), body.name)
    return {"tried": len(persons), "success": success, "body": body.name}
