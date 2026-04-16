# Mandari Prefect Ingestor

> Zentraler OParl-Aggregator für deutsche Kommunen.
> Liest OParl 1.0/1.1-Schnittstellen, reichert die Daten an (OCR, Personenbilder, etc.) und stellt sie als **eine** vereinheitlichte OParl-API für alle Kommunen bereit.

---

## Was es macht

1. **Liest OParl-Schnittstellen** vieler deutscher Kommunen parallel ein
2. **Speichert die Daten** strukturiert in PostgreSQL (JSONB + relationale Indizes)
3. **Verarbeitet sie weiter**:
   - PDF-Volltextextraktion (pypdf → OCR-Fallback)
   - Personenbilder herunterladen
   - Geo-Anreicherung von Vorlagen-Standorten
4. **Stellt sie bereit als**:
   - **Aggregierte OParl-API** (`/api/oparl/*`) — nach OParl-1.1-Spec, Read-Only
   - **Management-API** (`/api/admin/*`) — Quellen verwalten, Jobs triggern, Logs

---

## Architektur

```
┌─────────────────────────────────────────────────────┐
│  OParl-APIs deutscher Kommunen (extern)             │
│  Köln · Bonn · Münster · Aachen · Berlin · ...      │
└──────────────────┬──────────────────────────────────┘
                   │ httpx async, parallel
                   ▼
┌─────────────────────────────────────────────────────┐
│  Prefect Flows (Orchestrierung)                     │
│  ├─ sync_all_active_sources  (alle 10 Min)          │
│  ├─ full_sync                (täglich 3 Uhr)        │
│  ├─ extract_text             (Trigger nach Sync)    │
│  └─ fetch_person_photos      (täglich)              │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│  PostgreSQL (JSONB + relational indexes)            │
│  Bodies · Organizations · Persons · Memberships     │
│  Meetings · AgendaItems · Papers · Files · ...      │
│  + sync_logs · extraction_jobs                      │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│  FastAPI                                            │
│  ├─ /api/oparl/*   Aggregierte OParl-API (öffentl.) │
│  └─ /api/admin/*   Management (API-Key)             │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
        ┌──────────────────┐
        │   Mandari        │
        │   (oder andere   │
        │    Konsumenten)  │
        └──────────────────┘
```

---

## Tech-Stack

| Komponente | Technologie |
|------------|-------------|
| **Orchestrierung** | [Prefect 3](https://www.prefect.io) |
| **API** | [FastAPI](https://fastapi.tiangolo.com) |
| **HTTP-Client** | [httpx](https://www.python-httpx.org) (async) |
| **Datenbank** | PostgreSQL 16 (JSONB) |
| **ORM** | SQLAlchemy 2.0 + asyncpg |
| **Migrations** | Alembic |
| **Validation** | Pydantic v2 |
| **Package-Manager** | [uv](https://docs.astral.sh/uv) |
| **Lint/Format** | ruff |
| **Type-Check** | mypy |
| **Tests** | pytest + pytest-asyncio |
| **Container** | Docker + docker-compose |
| **CI/CD** | GitHub Actions |
| **Lizenz** | AGPL-3.0 |

---

## Quickstart (Dev)

```bash
# Repo klonen
git clone https://github.com/mandariOSS/prefect-ingestor.git
cd prefect-ingestor

# Mit Docker (empfohlen)
docker compose up -d

# Lokal entwickeln
uv sync
uv run alembic upgrade head
uv run prefect server start &
uv run uvicorn ingestor.api.main:app --reload --port 8080

# Erste Quelle hinzufügen
curl -X POST http://localhost:8080/api/admin/sources \
  -H "X-API-Key: dev-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"Stadt Münster","system_url":"https://oparl.stadt-muenster.de/system"}'

# Sync auslösen
curl -X POST http://localhost:8080/api/admin/sources/<id>/sync -H "X-API-Key: dev-key"

# Daten abfragen (OParl-Spec)
curl http://localhost:8080/api/oparl/system
```

---

## Status

🚧 **Aktiver Aufbau** — siehe [Roadmap](docs/ROADMAP.md)

| Phase | Status |
|-------|--------|
| Skelett + DB-Layer | 🚧 in Arbeit |
| OParl-Client + Flows | 📋 geplant |
| Aggregierte OParl-API | 📋 geplant |
| Management-API | 📋 geplant |
| Personenbilder + Geo | 📋 geplant |
| OCR-Pipeline | 📋 geplant |
| Mandari-Integration | 📋 geplant |
| Production-Deploy | 📋 geplant |

---

## Lizenz

[AGPL-3.0-or-later](LICENSE) — wie alle Mandari-Komponenten.

## Verwandte Projekte

- **[mandari](https://github.com/mandariOSS/mandari)** — Hauptanwendung (Insight + Work + Session Portal)
- **[mandari-oparl](https://github.com/mandariOSS/mandari)** (shared package im mandari-Repo) — Pydantic-Schemas für OParl
