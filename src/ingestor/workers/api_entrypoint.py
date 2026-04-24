"""
API-Entrypoint: führt Alembic-Migration auf head aus, dann startet uvicorn.

Schützt vor dem Schema-/Code-Drift-Klassiker: Image wird deployed, Schema ist
alt, API wirft IntegrityErrors. Hier ist das impossible — Migration ist
vor jedem Start idempotent.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

logger = logging.getLogger(__name__)


def _find_alembic_ini() -> Path | None:
    """Sucht alembic.ini in bekannten Pfaden (Container und lokal)."""
    env_path = os.environ.get("ALEMBIC_CONFIG")
    candidates = [
        Path(env_path) if env_path else None,
        Path("/app/alembic.ini"),
        Path.cwd() / "alembic.ini",
        Path(__file__).resolve().parents[4] / "alembic.ini",
    ]
    for c in candidates:
        if c and c.exists():
            return c
    return None


def run_migrations() -> None:
    cfg_path = _find_alembic_ini()
    if cfg_path is None:
        logger.warning("alembic.ini nicht gefunden — überspringe Migration")
        return
    cfg = Config(str(cfg_path))
    # script_location in der ini ist relativ zur ini
    cfg.set_main_option("script_location", str(cfg_path.parent / "src" / "ingestor" / "db" / "migrations"))
    logger.info("Running Alembic migrations from %s to head...", cfg_path)
    command.upgrade(cfg, "head")
    logger.info("Migrations done")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    try:
        run_migrations()
    except Exception:
        logger.exception("Migration failed — abort")
        sys.exit(1)

    import uvicorn

    from ingestor.config import get_settings

    settings = get_settings()
    uvicorn.run(
        "ingestor.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
