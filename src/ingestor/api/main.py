"""
FastAPI Hauptanwendung.

Sicherheit:
- CORS ist pro Router-Prefix konfigurierbar: `/api/oparl` erlaubt alle
  Origins (OParl-Spec verlangt das), `/api/admin` nur explizite Origins.
- Rate-Limiting via slowapi auf allen Routen (Default) und strenger auf
  Auth-Failures.
- Startup-Guard: refuse to start, wenn API_KEY default ist und nicht
  explizit DEV_MODE=1 gesetzt wurde.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from ingestor import __version__
from ingestor.api.management.router import router as management_router
from ingestor.api.oparl.router import router as oparl_router
from ingestor.config import get_settings
from ingestor.db import check_db

logger = structlog.get_logger(__name__)
_logger = logging.getLogger(__name__)


# =============================================================================
# Rate-Limiter (wird an App gebunden)
# =============================================================================

# Key per IP (X-Forwarded-For aware via get_remote_address)
limiter = Limiter(key_func=get_remote_address, default_limits=[])


# =============================================================================
# Pure-ASGI Middlewares — KEIN BaseHTTPMiddleware
# -----------------------------------------------------------------------------
# BaseHTTPMiddleware erzeugt interne anyio-TaskGroups, die SQLAlchemy-Engine-
# Pools in separate Event-Loops ziehen → "Task attached to a different loop".
# Pure ASGI umgeht das, indem es im gleichen Loop wie die App läuft.
# =============================================================================


class AdminCorsMiddleware:
    """
    Strikt für /api/admin: nur explizite Origins mit Credentials.
    /api/oparl wird separat via CORSMiddleware (offen, ohne Credentials) bedient.
    """

    def __init__(self, app, allowed_origins: list[str]) -> None:
        self.app = app
        self.allowed_origins = set(allowed_origins)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        is_admin = path.startswith("/api/admin")
        if not is_admin:
            await self.app(scope, receive, send)
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        origin = headers.get("origin")
        method = scope.get("method", "GET")

        # Preflight OPTIONS
        if method == "OPTIONS":
            if origin and origin in self.allowed_origins:
                await send({
                    "type": "http.response.start",
                    "status": 204,
                    "headers": [
                        (b"access-control-allow-origin", origin.encode()),
                        (b"access-control-allow-credentials", b"true"),
                        (b"access-control-allow-methods", b"GET,POST,PATCH,DELETE"),
                        (b"access-control-allow-headers", b"X-API-Key,Content-Type"),
                        (b"access-control-max-age", b"600"),
                        (b"vary", b"Origin"),
                    ],
                })
                await send({"type": "http.response.body", "body": b""})
                return
            await send({"type": "http.response.start", "status": 403,
                        "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body", "body": b'{"detail":"Origin not allowed"}'})
            return

        # Normaler Request: Response-Header ergänzen
        async def send_wrapper(message):
            if message["type"] == "http.response.start" and origin and origin in self.allowed_origins:
                headers_list = list(message.get("headers", []))
                headers_list.append((b"access-control-allow-origin", origin.encode()))
                headers_list.append((b"access-control-allow-credentials", b"true"))
                headers_list.append((b"vary", b"Origin"))
                message["headers"] = headers_list
            await send(message)

        await self.app(scope, receive, send_wrapper)


class SecurityHeadersMiddleware:
    """Setzt X-Content-Type-Options, X-Frame-Options, Referrer-Policy auf allen Responses."""

    _HEADERS_TO_ADD = [
        (b"x-content-type-options", b"nosniff"),
        (b"x-frame-options", b"DENY"),
        (b"referrer-policy", b"no-referrer"),
    ]

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                existing = {name.lower() for name, _ in message.get("headers", [])}
                headers_list = list(message.get("headers", []))
                for name, value in self._HEADERS_TO_ADD:
                    if name not in existing:
                        headers_list.append((name, value))
                message["headers"] = headers_list
            await send(message)

        await self.app(scope, receive, send_wrapper)


# =============================================================================
# Startup / Shutdown
# =============================================================================


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()

    # Startup-Guard: Default-Key nur in dev_mode
    if settings.api_key == "change-me" and not settings.dev_mode:
        raise RuntimeError(
            "Refusing to start: API_KEY ist der Default. "
            "Setze API_KEY=<starker Key> oder DEV_MODE=true in der Umgebung."
        )
    if settings.api_key == "change-me-to-a-strong-key":
        _logger.warning(
            "API_KEY ist der Beispiel-Wert — bitte vor Produktions-Deploy ersetzen "
            "und via POST /api/admin/api-keys einen echten Admin-Key anlegen."
        )

    logger.info(
        "ingestor.api.startup",
        version=__version__,
        db=str(settings.database_url).split("@")[-1],
    )
    yield
    logger.info("ingestor.api.shutdown")


# =============================================================================
# App-Factory
# =============================================================================


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Mandari Prefect Ingestor",
        description=(
            "Zentraler OParl-Aggregator für deutsche Kommunen.\n\n"
            "**Aggregierte OParl-API**: `/api/oparl/*` (öffentlich, OParl 1.1 Spec)\n\n"
            "**Management-API**: `/api/admin/*` (Auth via X-API-Key Header)"
        ),
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Rate-Limiter an App binden
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # /api/oparl: OParl-Spec verlangt CORS: *
    # Wichtig: allow_credentials=False für "*" (sonst invalid)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET", "HEAD"],
        allow_headers=["*"],
        expose_headers=["Content-Type"],
    )

    # /api/admin: strikt mit Credentials
    app.add_middleware(
        AdminCorsMiddleware,
        allowed_origins=settings.admin_cors_origins_list,
    )

    app.add_middleware(SecurityHeadersMiddleware)

    # =========================================================================
    # Health-Endpoints (ohne Auth, ohne Rate-Limit)
    # =========================================================================

    @app.get("/health", tags=["meta"])
    async def health() -> JSONResponse:
        db_ok = await check_db()
        return JSONResponse(
            status_code=200 if db_ok else 503,
            content={
                "status": "ok" if db_ok else "degraded",
                "version": __version__,
                "db": "ok" if db_ok else "unreachable",
            },
        )

    @app.get("/health/live", tags=["meta"])
    async def live() -> dict:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["meta"])
    async def ready() -> JSONResponse:
        db_ok = await check_db()
        return JSONResponse(
            status_code=200 if db_ok else 503,
            content={"ready": db_ok, "db": "ok" if db_ok else "unreachable"},
        )

    # =========================================================================
    # Router + Rate-Limits
    # =========================================================================

    app.include_router(oparl_router, prefix="/api/oparl")
    app.include_router(management_router, prefix="/api/admin")

    return app


app = create_app()


def run() -> None:
    """Entry-Point für `ingestor-api` Script."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "ingestor.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
