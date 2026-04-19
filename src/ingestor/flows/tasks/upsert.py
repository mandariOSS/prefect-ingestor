"""
Generischer Upsert-Task für OParl-Entitäten.

Speichert ein OParl-JSON-Objekt in die passende DB-Tabelle (basierend
auf detect_oparl_type) und gibt die UUID des gespeicherten Eintrags zurück.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from dateutil.parser import isoparse
from prefect import task
from sqlalchemy import select

from ingestor.db import get_session
from ingestor.db.models import (
    AgendaItem,
    Body,
    Consultation,
    File,
    LegislativeTerm,
    Location,
    Meeting,
    Membership,
    Organization,
    Paper,
    Person,
)
from ingestor.oparl.types import OParlType, detect_oparl_type

# Mapping OParl-Typ → SQLAlchemy-Modell
TYPE_MODEL_MAP: dict[OParlType, type] = {
    OParlType.BODY: Body,
    OParlType.ORGANIZATION: Organization,
    OParlType.PERSON: Person,
    OParlType.MEMBERSHIP: Membership,
    OParlType.MEETING: Meeting,
    OParlType.AGENDA_ITEM: AgendaItem,
    OParlType.PAPER: Paper,
    OParlType.CONSULTATION: Consultation,
    OParlType.FILE: File,
    OParlType.LOCATION: Location,
    OParlType.LEGISLATIVE_TERM: LegislativeTerm,
}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return isoparse(str(value))
    except (ValueError, TypeError):
        return None


def _parse_date(value: Any):
    dt = _parse_dt(value)
    return dt.date() if dt else None


@task(name="upsert-oparl-entity", retries=2, retry_delay_seconds=5)
async def upsert_entity(
    data: dict,
    body_id: UUID | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> UUID | None:
    """
    Upsert eines OParl-Objekts.

    Args:
        data: OParl-JSON-Objekt mit `id`, `type`, …
        body_id: Mandari-DB-UUID des zugehörigen Bodies (optional)
        extra_fields: Zusätzliche Felder für die Insert/Update

    Returns:
        UUID des Eintrags oder None bei Fehler.
    """
    oparl_type = detect_oparl_type(data)
    if not oparl_type:
        return None

    model = TYPE_MODEL_MAP.get(oparl_type)
    if not model:
        return None

    external_id = data.get("id")
    if not external_id:
        return None

    fields: dict[str, Any] = {
        "external_id": external_id,
        "raw": data,
        "oparl_created": _parse_dt(data.get("created")),
        "oparl_modified": _parse_dt(data.get("modified")),
        "deleted": data.get("deleted", False),
    }
    if body_id and hasattr(model, "body_id"):
        fields["body_id"] = body_id

    # Typ-spezifische Felder mappen
    fields.update(_extract_type_specific_fields(oparl_type, data))

    if extra_fields:
        fields.update(extra_fields)

    async with get_session() as session:
        # Suche bestehenden Eintrag nach (body_id, external_id) oder nur external_id
        if body_id and hasattr(model, "body_id"):
            stmt = select(model).where(
                model.body_id == body_id,
                model.external_id == external_id,
            )
        else:
            stmt = select(model).where(model.external_id == external_id)

        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            # WICHTIG: Enrichment-Spalten NICHT überschreiben!
            # Diese werden von OCR/Geo/Photo-Workern unabhängig gesetzt.
            ENRICHMENT_FIELDS = frozenset(
                {
                    "text_content",
                    "text_extraction_status",
                    "text_extraction_method",
                    "text_extraction_error",
                    "text_extracted_at",
                    "page_count",
                    "sha256_hash",
                    "latitude",
                    "longitude",
                    "photo_url",
                    "photo_data",
                    "photo_mime_type",
                    "photo_downloaded_at",
                }
            )
            for key, value in fields.items():
                if key not in ENRICHMENT_FIELDS:
                    setattr(existing, key, value)
            await session.flush()
            return existing.id
        else:
            obj = model(**fields)
            session.add(obj)
            await session.flush()
            return obj.id


def _extract_type_specific_fields(oparl_type: OParlType, data: dict) -> dict[str, Any]:
    """Extrahiert relationale Felder pro OParl-Typ aus dem JSON."""
    fields: dict[str, Any] = {}

    if oparl_type == OParlType.BODY:
        fields["name"] = data.get("name") or ""
        fields["short_name"] = data.get("shortName")
        fields["website"] = data.get("website")

    elif oparl_type == OParlType.ORGANIZATION:
        fields["name"] = data.get("name") or ""
        fields["short_name"] = data.get("shortName")
        fields["organization_type"] = data.get("organizationType")
        fields["classification"] = data.get("classification")
        fields["start_date"] = _parse_date(data.get("startDate"))
        fields["end_date"] = _parse_date(data.get("endDate"))

    elif oparl_type == OParlType.PERSON:
        fields["name"] = data.get("name") or ""
        fields["family_name"] = data.get("familyName")
        fields["given_name"] = data.get("givenName")
        fields["title"] = (
            data.get("title", [None])[0] if isinstance(data.get("title"), list) else data.get("title")
        )
        fields["email"] = (
            data.get("email", [None])[0] if isinstance(data.get("email"), list) else data.get("email")
        )
        fields["phone"] = (
            data.get("phone", [None])[0] if isinstance(data.get("phone"), list) else data.get("phone")
        )

    elif oparl_type == OParlType.MEMBERSHIP:
        fields["role"] = data.get("role")
        fields["voting_right"] = data.get("votingRight", True)
        fields["start_date"] = _parse_date(data.get("startDate"))
        fields["end_date"] = _parse_date(data.get("endDate"))

    elif oparl_type == OParlType.MEETING:
        fields["name"] = data.get("name")
        fields["start"] = _parse_dt(data.get("start"))
        fields["end"] = _parse_dt(data.get("end"))
        fields["cancelled"] = data.get("cancelled", False)
        fields["meeting_state"] = data.get("meetingState")
        location = data.get("location")
        if isinstance(location, dict):
            fields["location_name"] = location.get("description")
            fields["location_address"] = location.get("streetAddress")

    elif oparl_type == OParlType.AGENDA_ITEM:
        fields["number"] = data.get("number")
        fields["name"] = data.get("name")
        fields["public"] = data.get("public", True)
        fields["result"] = data.get("result")
        fields["resolution_text"] = data.get("resolutionText")
        fields["order"] = data.get("order")

    elif oparl_type == OParlType.PAPER:
        fields["name"] = data.get("name")
        fields["reference"] = data.get("reference")
        fields["paper_type"] = data.get("paperType")
        fields["date"] = _parse_date(data.get("date"))

    elif oparl_type == OParlType.CONSULTATION:
        fields["role"] = data.get("role")
        fields["authoritative"] = data.get("authoritative", False)

    elif oparl_type == OParlType.FILE:
        fields["name"] = data.get("name")
        fields["file_name"] = data.get("fileName")
        fields["mime_type"] = data.get("mimeType")
        fields["size"] = data.get("size")
        fields["access_url"] = data.get("accessUrl")
        fields["download_url"] = data.get("downloadUrl")
        fields["file_date"] = _parse_date(data.get("date"))
        fields["sha256_hash"] = data.get("sha256Checksum")

    elif oparl_type == OParlType.LOCATION:
        fields["description"] = data.get("description")
        fields["street_address"] = data.get("streetAddress")
        fields["locality"] = data.get("locality")
        fields["postal_code"] = data.get("postalCode")
        fields["region"] = data.get("region")
        geo = data.get("geojson")
        if isinstance(geo, dict):
            geometry = geo.get("geometry") or {}
            coords = geometry.get("coordinates") or []
            if len(coords) >= 2:
                fields["longitude"], fields["latitude"] = coords[0], coords[1]

    elif oparl_type == OParlType.LEGISLATIVE_TERM:
        fields["name"] = data.get("name") or ""
        fields["start_date"] = _parse_date(data.get("startDate"))
        fields["end_date"] = _parse_date(data.get("endDate"))

    return fields
