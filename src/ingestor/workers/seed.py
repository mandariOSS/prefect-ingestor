"""
Seed-Modul: füllt die Source-Tabelle mit einer vordefinierten Liste deutscher
OParl-Quellen, falls die DB leer ist.

Die Liste liegt als CSV unter `data/sources.csv` (name, system_url). Beim
Aufruf von `seed_sources_if_empty()` wird geprüft, ob bereits Sources
existieren — nur dann tut die Funktion nichts. Sonst werden alle CSV-Einträge
als `is_active=False` angelegt (manuell aktivieren, verhindert Überraschungs-
Lasten).
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

from sqlalchemy import func, select

from ingestor.db import get_session
from ingestor.db.models import Source

logger = logging.getLogger(__name__)


def _resolve_default_csv() -> Path:
    """Sucht sources.csv in bekannten Pfaden (Container + lokal entwickelt)."""
    env_path = os.environ.get("SOURCES_CSV")
    candidates = [
        Path(env_path) if env_path else None,
        Path("/app/data/sources.csv"),
        Path.cwd() / "data" / "sources.csv",
        Path(__file__).resolve().parents[4] / "data" / "sources.csv",
    ]
    for c in candidates:
        if c and c.exists():
            return c
    # Fallback (existiert nicht, seed_sources_if_empty warnt dann)
    return Path("/app/data/sources.csv")


DEFAULT_CSV_PATH = _resolve_default_csv()


async def count_sources() -> int:
    async with get_session() as session:
        return (await session.execute(select(func.count()).select_from(Source))).scalar_one()


async def seed_sources_if_empty(csv_path: Path | None = None) -> dict:
    """
    Legt Sources aus CSV an, falls die sources-Tabelle leer ist.

    Returns: {"inserted": N} oder {"skipped": "db not empty"} /
             {"skipped": "csv missing"}.
    """
    path = csv_path or DEFAULT_CSV_PATH
    if not path.exists():
        logger.info("Seed skipped — CSV nicht gefunden: %s", path)
        return {"skipped": "csv missing", "path": str(path)}

    existing = await count_sources()
    if existing > 0:
        logger.info("Seed skipped — %d Sources bereits in DB", existing)
        return {"skipped": "db not empty", "existing": existing}

    inserted = 0
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        async with get_session() as session:
            for row in reader:
                name = (row.get("name") or "").strip()
                url = (row.get("system_url") or "").strip()
                if not name or not url:
                    continue
                # is_active=False default — Betreiber aktiviert manuell
                session.add(Source(name=name, system_url=url, is_active=False))
                inserted += 1

    logger.info("Seed: %d Sources aus %s angelegt (is_active=False)", inserted, path.name)
    return {"inserted": inserted, "path": str(path)}


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(asyncio.run(seed_sources_if_empty()))
