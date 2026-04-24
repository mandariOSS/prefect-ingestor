"""
Auth für die Management-API.

Zwei Auth-Modi — beide kompatibel via `X-API-Key` Header:

1. **DB-Keys** (Produktion): Key wird in DB per bcrypt gehashed, hat Name,
   Scopes, kann widerrufen/ablaufen. Jede Verwendung wird in `audit_logs`
   protokolliert.
2. **Bootstrap-Key** (Fallback): Wenn keine DB-Keys existieren, akzeptiert
   die API den Klartext-Key aus `settings.api_key` — aber **nur wenn der
   Key nicht der Default "change-me" ist** oder `DEV_MODE=1` gesetzt ist.
   Dieser Fallback hat implizit admin-Scope und dient dem ersten Login, um
   einen echten Admin-Key in der DB anzulegen.

Scopes (alphabetisch):
    - read   : GET-Endpoints auf Sources/Logs/Stats
    - write  : POST/PATCH/DELETE Sources, Sync-Trigger, Enrichment
    - admin  : api-keys-CRUD, audit-logs lesen, destructive Ops

admin > write > read (hierarchisch). Wer `write` hat, kann `read`. Wer
`admin` hat, kann alles.
"""

from __future__ import annotations

import hmac
import logging
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated

import bcrypt
from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor.api.dependencies import SessionDep
from ingestor.config import Settings, get_settings
from ingestor.db.models import ApiKey, AuditLog

logger = logging.getLogger(__name__)

SCOPE_HIERARCHY = {"read": 0, "write": 1, "admin": 2}
KEY_PREFIX_LENGTH = 8


@dataclass
class AuthContext:
    """Ergebnis einer erfolgreichen Auth — was der Key darf, wer er ist."""

    api_key_id: str | None  # None = Bootstrap-Key aus env
    actor: str  # Display-Name fürs Audit-Log
    scopes: set[str]
    ip: str | None

    def has_scope(self, required: str) -> bool:
        required_level = SCOPE_HIERARCHY[required]
        return any(SCOPE_HIERARCHY.get(s, -1) >= required_level for s in self.scopes)


# =============================================================================
# Key-Generation + Hashing
# =============================================================================


def generate_api_key() -> str:
    """Erzeugt einen kryptographisch sicheren Key. Nicht URL-safe-Base64,
    um Verwechslung mit Token anderer Systeme zu vermeiden."""
    return "mi_" + secrets.token_urlsafe(32)


def _bcrypt_rounds() -> int:
    """Test-Override via BCRYPT_ROUNDS (z. B. 4 für Tests), sonst 12."""
    try:
        return int(os.environ.get("BCRYPT_ROUNDS", "12"))
    except ValueError:
        return 12


def hash_api_key(plaintext: str) -> str:
    return bcrypt.hashpw(plaintext.encode("utf-8"), bcrypt.gensalt(rounds=_bcrypt_rounds())).decode("utf-8")


def verify_api_key(plaintext: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plaintext.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def key_prefix(plaintext: str) -> str:
    return plaintext[:KEY_PREFIX_LENGTH]


# =============================================================================
# Lookup + Validation
# =============================================================================


async def _count_active_keys(session: AsyncSession) -> int:
    stmt = select(func.count()).select_from(ApiKey).where(ApiKey.revoked_at.is_(None))
    return (await session.execute(stmt)).scalar_one()


async def _authenticate_bootstrap(key: str, settings: Settings, ip: str | None) -> AuthContext | None:
    """Akzeptiert Key aus settings.api_key NUR wenn DB leer ist."""
    if not settings.api_key:
        return None
    # Default-Key nur in dev_mode erlauben
    if settings.api_key == "change-me" and not settings.dev_mode:
        return None
    # Constant-time compare
    if not hmac.compare_digest(key, settings.api_key):
        return None
    return AuthContext(
        api_key_id=None,
        actor="bootstrap",
        scopes={"admin"},
        ip=ip,
    )


async def _authenticate_db(key: str, session: AsyncSession, ip: str | None) -> AuthContext | None:
    """Sucht Key in DB. Wir suchen über Prefix, dann bcrypt-verify."""
    prefix = key_prefix(key)
    stmt = (
        select(ApiKey)
        .where(ApiKey.prefix == prefix)
        .where(ApiKey.revoked_at.is_(None))
    )
    candidates = (await session.execute(stmt)).scalars().all()

    for candidate in candidates:
        # Ablauf-Check
        if candidate.expires_at and candidate.expires_at < datetime.now(UTC):
            continue
        if verify_api_key(key, candidate.key_hash):
            # Async fire-and-forget: last_used aktualisieren
            await session.execute(
                update(ApiKey)
                .where(ApiKey.id == candidate.id)
                .values(last_used_at=datetime.now(UTC), last_used_ip=ip)
            )
            return AuthContext(
                api_key_id=str(candidate.id),
                actor=candidate.name,
                scopes=set(s.strip() for s in candidate.scopes.split(",") if s.strip()),
                ip=ip,
            )
    return None


def _client_ip(request: Request) -> str | None:
    """Client-IP — bevorzugt X-Forwarded-For Erste-Eintrag, sonst request.client."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


# =============================================================================
# FastAPI Dependencies
# =============================================================================


async def authenticate(
    request: Request,
    session: SessionDep,
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> AuthContext:
    """Hauptsächliche Auth-Dependency. Gibt AuthContext zurück oder 401."""
    if not x_api_key or len(x_api_key) < 8:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or malformed X-API-Key header",
        )

    ip = _client_ip(request)

    # Erst DB-Keys probieren
    auth = await _authenticate_db(x_api_key, session, ip)
    if auth is not None:
        return auth

    # Bootstrap-Fallback: nur wenn keine aktiven DB-Keys existieren
    active_keys = await _count_active_keys(session)
    if active_keys == 0:
        auth = await _authenticate_bootstrap(x_api_key, settings, ip)
        if auth is not None:
            logger.warning(
                "Bootstrap-API-Key verwendet von %s — bitte echten Admin-Key in DB anlegen",
                ip or "unknown",
            )
            return auth

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
    )


def require_scope(scope: str):
    """Factory für Scope-basierte Dependencies."""

    async def _check(auth: Annotated[AuthContext, Depends(authenticate)]) -> AuthContext:
        if not auth.has_scope(scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Scope '{scope}' erforderlich (vorhanden: {sorted(auth.scopes)})",
            )
        return auth

    return _check


# Shorthand-Dependencies
RequireRead = Annotated[AuthContext, Depends(require_scope("read"))]
RequireWrite = Annotated[AuthContext, Depends(require_scope("write"))]
RequireAdmin = Annotated[AuthContext, Depends(require_scope("admin"))]


# =============================================================================
# Audit-Logging
# =============================================================================


async def write_audit_log(
    session: AsyncSession,
    auth: AuthContext,
    action: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    status_code: int | None = None,
    details: dict | None = None,
) -> None:
    """Schreibt einen Audit-Eintrag. Nie-werfend (Audit darf nicht User-Flow brechen)."""
    try:
        entry = AuditLog(
            api_key_id=auth.api_key_id,
            actor=auth.actor,
            action=action,
            resource_type=resource_type,
            resource_id=str(resource_id) if resource_id else None,
            ip_address=auth.ip,
            status_code=status_code,
            details=details or {},
        )
        session.add(entry)
        await session.flush()
    except Exception:
        logger.exception("Failed to write audit log (non-fatal)")
