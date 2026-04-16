"""
FastAPI Hauptanwendung.

Mountet zwei Subapps:
- /api/oparl   — Aggregierte OParl-API (read-only, OParl-1.1 Spec)
- /api/admin   — Management-API (auth required)
"""

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ingestor import __version__
from ingestor.api.management.router import router as management_router
from ingestor.api.oparl.router import router as oparl_router
from ingestor.config import get_settings

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    logger.info("ingestor.api.startup", version=__version__, db=str(settings.database_url).split("@")[-1])
    yield
    logger.info("ingestor.api.shutdown")


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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    @app.get("/health", tags=["meta"])
    async def health() -> dict:
        return {"status": "ok", "version": __version__}

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
