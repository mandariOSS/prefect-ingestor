"""
OParl-1.1-konforme öffentliche API.

Liefert die aggregierten Daten ALLER eingelesenen Kommunen als
einzelne OParl-API. Der `system`-Endpoint listet alle Bodies auf.

Vorteil: Konsumenten müssen nur eine API integrieren.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from dateutil.parser import isoparse
from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import select

from ingestor.api.dependencies import SessionDep
from ingestor.db.models import (
    Body,
    File,
    Meeting,
    Organization,
    Paper,
    Person,
)

router = APIRouter(tags=["oparl"])

PAGE_SIZE = 100


def _self_url(request: Request, path: str) -> str:
    """Konstruiert die self-URL für OParl-Antworten."""
    return str(request.url_for("oparl_root")).rstrip("/") + path


def _parse_modified_since(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return isoparse(value)
    except (ValueError, TypeError):
        raise HTTPException(400, "Invalid 'modified_since' (ISO-8601 erforderlich)") from None


def _wrap_list(items: list[dict], request: Request, path: str, page: int, total: int | None) -> dict:
    """Wrappt eine Liste in das OParl-Pagination-Format."""
    base = _self_url(request, path)
    sep = "&" if "?" in path else "?"
    next_url: str | None = None
    if total is None or len(items) == PAGE_SIZE:
        next_url = f"{base}{sep}page={page + 1}"
    return {
        "data": items,
        "pagination": {
            "totalElements": total,
            "elementsPerPage": PAGE_SIZE,
            "currentPage": page,
        },
        "links": {"self": f"{base}{sep}page={page}", **({"next": next_url} if next_url else {})},
    }


# =============================================================================
# /system
# =============================================================================


@router.get("/system", name="oparl_root")
async def get_system(request: Request, session: SessionDep) -> dict:
    """OParl System-Endpoint — listet alle aggregierten Bodies."""
    base = str(request.base_url).rstrip("/")
    return {
        "id": _self_url(request, "/system"),
        "type": "https://schema.oparl.org/1.1/System",
        "oparlVersion": "https://schema.oparl.org/1.1/",
        "name": "Mandari Aggregator",
        "contactName": "Mandari",
        "contactEmail": "hello@mandari.de",
        "website": base,
        "vendor": "https://github.com/mandariOSS/prefect-ingestor",
        "product": "Mandari Prefect Ingestor",
        "body": _self_url(request, "/bodies"),
    }


# =============================================================================
# /bodies — Liste aller Bodies (über alle Quellen)
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
    stmt = stmt.order_by(Body.name).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)

    result = await session.execute(stmt)
    bodies = result.scalars().all()
    return _wrap_list(
        [_serialize_body(b, request) for b in bodies],
        request,
        "/bodies",
        page,
        total=None,
    )


@router.get("/body/{body_id}")
async def get_body(body_id: UUID, request: Request, session: SessionDep) -> dict:
    body = (await session.execute(select(Body).where(Body.id == body_id))).scalar_one_or_none()
    if not body:
        raise HTTPException(404, "Body nicht gefunden")
    return _serialize_body(body, request)


def _serialize_body(body: Body, request: Request) -> dict:
    bid = str(body.id)
    return {
        "id": _self_url(request, f"/body/{bid}"),
        "type": "https://schema.oparl.org/1.1/Body",
        "name": body.name,
        "shortName": body.short_name,
        "website": body.website,
        "system": _self_url(request, "/system"),
        "organization": _self_url(request, f"/body/{bid}/organizations"),
        "person": _self_url(request, f"/body/{bid}/persons"),
        "meeting": _self_url(request, f"/body/{bid}/meetings"),
        "paper": _self_url(request, f"/body/{bid}/papers"),
        "legislativeTerm": _self_url(request, f"/body/{bid}/legislative-terms"),
        "location": _self_url(request, f"/body/{bid}/locations"),
        "created": body.oparl_created.isoformat() if body.oparl_created else None,
        "modified": body.oparl_modified.isoformat() if body.oparl_modified else None,
    }


# =============================================================================
# Generischer Listen-Endpoint pro Body
# =============================================================================


def _make_list_endpoint(model: type, name: str, serializer):
    async def endpoint(
        body_id: UUID,
        request: Request,
        session: SessionDep,
        page: int = Query(1, ge=1),
        modified_since: str | None = None,
    ) -> dict:
        ms = _parse_modified_since(modified_since)
        stmt = select(model).where(model.body_id == body_id, model.deleted.is_(False))
        if ms:
            stmt = stmt.where(model.oparl_modified >= ms)
        stmt = stmt.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)
        result = await session.execute(stmt)
        items = result.scalars().all()
        return _wrap_list(
            [serializer(it, request) for it in items],
            request,
            f"/body/{body_id}/{name}",
            page,
            total=None,
        )

    return endpoint


def _common_fields(obj: Any, request: Request, type_name: str, path: str) -> dict[str, Any]:
    return {
        "id": _self_url(request, path),
        "type": f"https://schema.oparl.org/1.1/{type_name}",
        "created": obj.oparl_created.isoformat() if obj.oparl_created else None,
        "modified": obj.oparl_modified.isoformat() if obj.oparl_modified else None,
        "deleted": obj.deleted,
    }


# Organizations
def _serialize_organization(o: Organization, request: Request) -> dict:
    return {
        **_common_fields(o, request, "Organization", f"/organization/{o.id}"),
        "name": o.name,
        "shortName": o.short_name,
        "organizationType": o.organization_type,
        "classification": o.classification,
        "startDate": o.start_date.isoformat() if o.start_date else None,
        "endDate": o.end_date.isoformat() if o.end_date else None,
    }


router.get("/body/{body_id}/organizations")(
    _make_list_endpoint(Organization, "organizations", _serialize_organization)
)


@router.get("/organization/{oid}")
async def get_organization(oid: UUID, request: Request, session: SessionDep) -> dict:
    obj = (await session.execute(select(Organization).where(Organization.id == oid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    return _serialize_organization(obj, request)


# Persons
def _serialize_person(p: Person, request: Request) -> dict:
    return {
        **_common_fields(p, request, "Person", f"/person/{p.id}"),
        "name": p.name,
        "familyName": p.family_name,
        "givenName": p.given_name,
        "title": [p.title] if p.title else None,
        "email": [p.email] if p.email else None,
    }


router.get("/body/{body_id}/persons")(_make_list_endpoint(Person, "persons", _serialize_person))


@router.get("/person/{pid}")
async def get_person(pid: UUID, request: Request, session: SessionDep) -> dict:
    obj = (await session.execute(select(Person).where(Person.id == pid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    return _serialize_person(obj, request)


# Meetings
def _serialize_meeting(m: Meeting, request: Request) -> dict:
    return {
        **_common_fields(m, request, "Meeting", f"/meeting/{m.id}"),
        "name": m.name,
        "start": m.start.isoformat() if m.start else None,
        "end": m.end.isoformat() if m.end else None,
        "cancelled": m.cancelled,
        "meetingState": m.meeting_state,
    }


router.get("/body/{body_id}/meetings")(_make_list_endpoint(Meeting, "meetings", _serialize_meeting))


@router.get("/meeting/{mid}")
async def get_meeting(mid: UUID, request: Request, session: SessionDep) -> dict:
    obj = (await session.execute(select(Meeting).where(Meeting.id == mid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    return _serialize_meeting(obj, request)


# Papers
def _serialize_paper(p: Paper, request: Request) -> dict:
    return {
        **_common_fields(p, request, "Paper", f"/paper/{p.id}"),
        "name": p.name,
        "reference": p.reference,
        "paperType": p.paper_type,
        "date": p.date.isoformat() if p.date else None,
    }


router.get("/body/{body_id}/papers")(_make_list_endpoint(Paper, "papers", _serialize_paper))


@router.get("/paper/{pid}")
async def get_paper(pid: UUID, request: Request, session: SessionDep) -> dict:
    obj = (await session.execute(select(Paper).where(Paper.id == pid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    return _serialize_paper(obj, request)


# Files (mit Mandari-Erweiterung: extrahierter Text)
@router.get("/file/{fid}")
async def get_file(fid: UUID, request: Request, session: SessionDep) -> dict:
    obj = (await session.execute(select(File).where(File.id == fid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    return {
        **_common_fields(obj, request, "File", f"/file/{fid}"),
        "name": obj.name,
        "fileName": obj.file_name,
        "mimeType": obj.mime_type,
        "size": obj.size,
        "accessUrl": obj.access_url,
        "downloadUrl": obj.download_url,
        "date": obj.file_date.isoformat() if obj.file_date else None,
        # Mandari-Erweiterungen:
        "x:textExtractionStatus": obj.text_extraction_status,
        "x:hasText": bool(obj.text_content),
        "x:textUrl": _self_url(request, f"/file/{fid}/text") if obj.text_content else None,
    }


@router.get("/file/{fid}/text")
async def get_file_text(fid: UUID, session: SessionDep) -> dict:
    """Mandari-Erweiterung: extrahierter Text einer Datei."""
    obj = (await session.execute(select(File).where(File.id == fid))).scalar_one_or_none()
    if not obj:
        raise HTTPException(404)
    if not obj.text_content:
        raise HTTPException(404, f"Kein Text vorhanden (Status: {obj.text_extraction_status})")
    return {
        "fileId": str(fid),
        "method": obj.text_extraction_method,
        "extractedAt": obj.text_extracted_at.isoformat() if obj.text_extracted_at else None,
        "pageCount": obj.page_count,
        "text": obj.text_content,
    }
