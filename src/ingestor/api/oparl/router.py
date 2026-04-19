"""
OParl-1.1-konforme öffentliche API.

Aggregiert ALLE eingelesenen Kommunen als eine OParl-Schnittstelle.
Jede Kommune ist ein Body unter /api/oparl/bodies.

WICHTIG: Daten werden 1:1 aus den Original-OParl-Quellen übernommen.
Keine eigenen Daten generiert. Eigene Ergänzungen (OCR-Text, Fotos)
werden als mandari:-Vendor-Extension markiert.

Die `raw`-JSONB-Spalte enthält das vollständige Original-JSON — wir
geben es direkt weiter und ersetzen nur die internen URLs.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from dateutil.parser import isoparse
from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import select

from ingestor.api.dependencies import SessionDep
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

router = APIRouter(tags=["oparl"])

PAGE_SIZE = 100


# =============================================================================
# URL-Helpers
# =============================================================================


def _base_url(request: Request) -> str:
    """OParl-API Base-URL."""
    return str(request.base_url).rstrip("/") + "/api/oparl"


def _oparl_url(request: Request, path: str) -> str:
    return _base_url(request) + path


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse_modified_since(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return isoparse(value)
    except (ValueError, TypeError):
        raise HTTPException(400, "Invalid 'modified_since' (ISO-8601 erforderlich)") from None


# =============================================================================
# Pagination (OParl 1.1 spec-konform)
# =============================================================================


def _paginated_list(
    items: list[dict],
    request: Request,
    path: str,
    page: int,
    total: int | None = None,
) -> dict:
    """OParl-1.1-konforme Pagination mit links.self/next/first/prev."""
    base = _oparl_url(request, path)
    has_next = len(items) == PAGE_SIZE

    links: dict[str, str] = {
        "first": f"{base}?page=1",
        "self": f"{base}?page={page}",
    }
    if page > 1:
        links["prev"] = f"{base}?page={page - 1}"
    if has_next:
        links["next"] = f"{base}?page={page + 1}"

    result: dict[str, Any] = {
        "data": items,
        "links": links,
    }

    pagination: dict[str, Any] = {
        "elementsPerPage": PAGE_SIZE,
        "currentPage": page,
    }
    if total is not None:
        pagination["totalElements"] = total
        pagination["totalPages"] = (total + PAGE_SIZE - 1) // PAGE_SIZE
    result["pagination"] = pagination

    return result


# =============================================================================
# Raw-JSON Durchreichen mit URL-Rewrite
# =============================================================================


def _rewrite_raw(raw: dict, request: Request, obj_id: UUID, type_path: str) -> dict:
    """
    Gibt das Original-OParl-JSON zurück, ersetzt aber die `id`-URL
    durch unsere aggregierte URL. Alle anderen Felder bleiben Original.
    """
    if not raw:
        return {}

    out = dict(raw)
    # Unsere aggregierte URL als id
    out["id"] = _oparl_url(request, f"/{type_path}/{obj_id}")
    return out


def _raw_with_mandari_extensions(
    raw: dict,
    request: Request,
    obj_id: UUID,
    type_path: str,
    extensions: dict[str, Any] | None = None,
) -> dict:
    """Raw-JSON + mandari:-Vendor-Extensions."""
    out = _rewrite_raw(raw, request, obj_id, type_path)
    if extensions:
        for key, value in extensions.items():
            if value is not None:
                out[f"mandari:{key}"] = value
    return out


# =============================================================================
# /system — Einstiegspunkt (OParl 1.1 Pflichtfelder)
# =============================================================================


@router.get("/system")
async def get_system(request: Request, session: SessionDep) -> dict:
    """OParl System-Endpoint. Listet `body`-URL zu allen aggregierten Bodies."""
    base = _base_url(request)
    now = datetime.now(UTC).isoformat()
    return {
        "id": f"{base}/system",
        "type": "https://schema.oparl.org/1.1/System",
        "oparlVersion": "https://schema.oparl.org/1.1/",
        "name": "Mandari OParl Aggregator",
        "contactName": "Mandari",
        "contactEmail": "hello@mandari.de",
        "website": str(request.base_url).rstrip("/"),
        "vendor": "https://github.com/mandariOSS/prefect-ingestor",
        "product": "https://github.com/mandariOSS/prefect-ingestor",
        "body": f"{base}/bodies",
        "created": now,
        "modified": now,
    }


# =============================================================================
# /bodies — Alle Kommunen als Bodies (über alle Quellen)
# =============================================================================


@router.get("/bodies")
async def list_bodies(
    request: Request,
    session: SessionDep,
    page: int = Query(1, ge=1),
    modified_since: str | None = None,
) -> dict:
    ms = _parse_modified_since(modified_since)
    stmt = select(Body).where(Body.deleted.is_(False))
    if ms:
        stmt = stmt.where(Body.oparl_modified >= ms)
    # Deleted mit ausgeben wenn modified_since gesetzt (OParl 1.1 Pflicht)
    if ms:
        stmt = select(Body).where(Body.oparl_modified >= ms)
    stmt = stmt.order_by(Body.name).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)

    result = await session.execute(stmt)
    bodies = result.scalars().all()
    return _paginated_list(
        [_serialize_body(b, request) for b in bodies],
        request,
        "/bodies",
        page,
    )


@router.get("/body/{body_id}")
async def get_body(body_id: UUID, request: Request, session: SessionDep) -> dict:
    body = (await session.execute(select(Body).where(Body.id == body_id))).scalar_one_or_none()
    if not body:
        raise HTTPException(404, "Body nicht gefunden")
    return _serialize_body(body, request)


def _serialize_body(body: Body, request: Request) -> dict:
    """Body aus raw-JSON + aggregierte Listen-URLs."""
    base = _base_url(request)
    bid = str(body.id)

    # Original-Daten aus raw übernehmen
    out = _rewrite_raw(body.raw, request, body.id, "body")

    # OParl 1.1 Pflicht-Listen-URLs auf unsere API umschreiben
    out.update(
        {
            "type": "https://schema.oparl.org/1.1/Body",
            "system": f"{base}/system",
            "organization": f"{base}/body/{bid}/organizations",
            "person": f"{base}/body/{bid}/persons",
            "meeting": f"{base}/body/{bid}/meetings",
            "paper": f"{base}/body/{bid}/papers",
            "legislativeTerm": f"{base}/body/{bid}/legislative-terms",
            "agendaItem": f"{base}/body/{bid}/agenda-items",
            "consultation": f"{base}/body/{bid}/consultations",
            "file": f"{base}/body/{bid}/files",
            "locationList": f"{base}/body/{bid}/locations",
            "legislativeTermList": f"{base}/body/{bid}/legislative-terms",
            "membership": f"{base}/body/{bid}/memberships",
            # Timestamps aus DB (nicht aus raw — raw könnte veraltet sein)
            "created": _dt(body.oparl_created),
            "modified": _dt(body.oparl_modified),
            "deleted": body.deleted,
        }
    )
    return out


# =============================================================================
# Generischer Listen-Endpoint-Factory
# =============================================================================


def _make_body_list_endpoint(model, type_path: str, serialize_fn):
    """Erstellt einen paginierten Listen-Endpoint für einen Body."""

    async def endpoint(
        body_id: UUID,
        request: Request,
        session: SessionDep,
        page: int = Query(1, ge=1),
        modified_since: str | None = None,
    ) -> dict:
        ms = _parse_modified_since(modified_since)
        stmt = select(model).where(model.body_id == body_id)
        if ms:
            # Mit modified_since: auch gelöschte Objekte (OParl 1.1)
            stmt = stmt.where(model.oparl_modified >= ms)
        else:
            stmt = stmt.where(model.deleted.is_(False))
        stmt = stmt.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)
        result = await session.execute(stmt)
        items = result.scalars().all()
        return _paginated_list(
            [serialize_fn(it, request) for it in items],
            request,
            f"/body/{body_id}/{type_path}",
            page,
        )

    return endpoint


def _make_detail_endpoint(model, type_path: str, serialize_fn):
    """Erstellt einen Detail-Endpoint."""

    async def endpoint(item_id: UUID, request: Request, session: SessionDep) -> dict:
        obj = (await session.execute(select(model).where(model.id == item_id))).scalar_one_or_none()
        if not obj:
            raise HTTPException(404)
        return serialize_fn(obj, request)

    return endpoint


# =============================================================================
# Serializer — geben raw-JSON 1:1 weiter, nur id/type umgeschrieben
# =============================================================================


def _serialize_organization(o: Organization, request: Request) -> dict:
    return _rewrite_raw(o.raw, request, o.id, "organization")


def _serialize_person(p: Person, request: Request) -> dict:
    extensions: dict[str, Any] = {}
    if p.photo_url:
        extensions["photoUrl"] = _oparl_url(request, f"/person/{p.id}/photo")
        extensions["photoMimeType"] = p.photo_mime_type
    return _raw_with_mandari_extensions(p.raw, request, p.id, "person", extensions)


def _serialize_membership(m: Membership, request: Request) -> dict:
    return _rewrite_raw(m.raw, request, m.id, "membership")


def _serialize_meeting(m: Meeting, request: Request) -> dict:
    return _rewrite_raw(m.raw, request, m.id, "meeting")


def _serialize_agenda_item(a: AgendaItem, request: Request) -> dict:
    return _rewrite_raw(a.raw, request, a.id, "agenda-item")


def _serialize_paper(p: Paper, request: Request) -> dict:
    return _rewrite_raw(p.raw, request, p.id, "paper")


def _serialize_consultation(c: Consultation, request: Request) -> dict:
    return _rewrite_raw(c.raw, request, c.id, "consultation")


def _serialize_file(f: File, request: Request) -> dict:
    extensions: dict[str, Any] = {}
    # OParl-Standard: `text`-Feld für extrahierten Text (kein Vendor-Prefix nötig!)
    out = _rewrite_raw(f.raw, request, f.id, "file")
    if f.text_content:
        out["text"] = f.text_content  # Standard-OParl-Feld!
    extensions["textExtractionStatus"] = f.text_extraction_status
    if extensions:
        for key, value in extensions.items():
            if value is not None:
                out[f"mandari:{key}"] = value
    return out


def _serialize_location(loc: Location, request: Request) -> dict:
    out = _rewrite_raw(loc.raw, request, loc.id, "location")
    # Geo-Daten aus Nominatim ins Standard-geojson-Feld (kein Vendor-Prefix!)
    if loc.latitude and loc.longitude and "geojson" not in out:
        out["geojson"] = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [loc.longitude, loc.latitude],
            },
        }
    return out


def _serialize_legislative_term(lt: LegislativeTerm, request: Request) -> dict:
    return _rewrite_raw(lt.raw, request, lt.id, "legislative-term")


# =============================================================================
# Routen registrieren — Listen + Detail für alle OParl-Typen
# =============================================================================

# Organizations
router.get("/body/{body_id}/organizations")(
    _make_body_list_endpoint(Organization, "organizations", _serialize_organization)
)
router.get("/organization/{item_id}")(
    _make_detail_endpoint(Organization, "organization", _serialize_organization)
)

# Persons
router.get("/body/{body_id}/persons")(_make_body_list_endpoint(Person, "persons", _serialize_person))
router.get("/person/{item_id}")(_make_detail_endpoint(Person, "person", _serialize_person))


# Person Photo (Mandari-Erweiterung, separater Endpoint)
@router.get("/person/{person_id}/photo")
async def get_person_photo(person_id: UUID, session: SessionDep):
    """Personenbild als Binary (Mandari-Erweiterung, nicht OParl-Standard)."""
    from fastapi.responses import Response

    person = (await session.execute(select(Person).where(Person.id == person_id))).scalar_one_or_none()
    if not person or not person.photo_data:
        raise HTTPException(404, "Kein Foto vorhanden")
    return Response(
        content=bytes(person.photo_data),
        media_type=person.photo_mime_type or "image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"},
    )


# Memberships
router.get("/body/{body_id}/memberships")(
    _make_body_list_endpoint(Membership, "memberships", _serialize_membership)
)
router.get("/membership/{item_id}")(_make_detail_endpoint(Membership, "membership", _serialize_membership))

# Meetings
router.get("/body/{body_id}/meetings")(_make_body_list_endpoint(Meeting, "meetings", _serialize_meeting))
router.get("/meeting/{item_id}")(_make_detail_endpoint(Meeting, "meeting", _serialize_meeting))

# AgendaItems
router.get("/body/{body_id}/agenda-items")(
    _make_body_list_endpoint(AgendaItem, "agenda-items", _serialize_agenda_item)
)
router.get("/agenda-item/{item_id}")(_make_detail_endpoint(AgendaItem, "agenda-item", _serialize_agenda_item))

# Papers
router.get("/body/{body_id}/papers")(_make_body_list_endpoint(Paper, "papers", _serialize_paper))
router.get("/paper/{item_id}")(_make_detail_endpoint(Paper, "paper", _serialize_paper))

# Consultations
router.get("/body/{body_id}/consultations")(
    _make_body_list_endpoint(Consultation, "consultations", _serialize_consultation)
)
router.get("/consultation/{item_id}")(
    _make_detail_endpoint(Consultation, "consultation", _serialize_consultation)
)

# Files
router.get("/body/{body_id}/files")(_make_body_list_endpoint(File, "files", _serialize_file))
router.get("/file/{item_id}")(_make_detail_endpoint(File, "file", _serialize_file))

# Locations
router.get("/body/{body_id}/locations")(_make_body_list_endpoint(Location, "locations", _serialize_location))
router.get("/location/{item_id}")(_make_detail_endpoint(Location, "location", _serialize_location))

# LegislativeTerms
router.get("/body/{body_id}/legislative-terms")(
    _make_body_list_endpoint(LegislativeTerm, "legislative-terms", _serialize_legislative_term)
)
router.get("/legislative-term/{item_id}")(
    _make_detail_endpoint(LegislativeTerm, "legislative-term", _serialize_legislative_term)
)
