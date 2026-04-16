# Architektur

## Überblick

Der **Mandari Prefect Ingestor** ist ein eigenständiger Microservice, der OParl-Daten deutscher Kommunen aggregiert und über eine einheitliche API bereitstellt.

```
┌──────────────────────────────────────────────────────┐
│                  EXTERNE OPARL-APIs                  │
│   Köln · Bonn · Münster · Aachen · Berlin · ...      │
└────────────────────┬─────────────────────────────────┘
                     │ httpx (async, http2)
                     ▼
┌──────────────────────────────────────────────────────┐
│                PREFECT FLOWS                         │
│  Schedules: incremental (10min) + full (3am)         │
│  Parallel via asyncio.gather                         │
│  Retries, Logs, Telemetry                            │
└────────────────────┬─────────────────────────────────┘
                     │ SQLAlchemy 2.0 async
                     ▼
┌──────────────────────────────────────────────────────┐
│             POSTGRESQL (JSONB + relational)          │
│  Bodies, Organizations, Persons, Memberships,        │
│  Meetings, AgendaItems, Papers, Files, ...           │
│  + sync_logs, extraction_jobs                        │
└────────────────────┬─────────────────────────────────┘
                     │
        ┌────────────┴─────────────┐
        ▼                          ▼
┌──────────────────┐    ┌─────────────────────┐
│  /api/oparl/*    │    │  /api/admin/*       │
│  Aggregierte     │    │  Management         │
│  OParl-1.1-API   │    │  (X-API-Key)        │
│  (read-only)     │    │  Sources, Logs      │
└──────────────────┘    └─────────────────────┘
        │                          │
        ▼                          ▼
   Mandari, Bürger,         Mandari-Admin
   Dritt-Apps               (Org-Settings)
```

## Komponenten

### Prefect (Orchestration)

- **Server**: Self-hosted unter `:4200` (oder Prefect Cloud)
- **Worker**: Container, der Flows ausführt
- **Schedules**: Cron-basiert, automatische Retries

**Wichtige Flows**:
- `sync_source_flow(source_id, full)` — eine Quelle synchronisieren
- `sync_all_active_sources(full)` — alle aktiven Quellen parallel
- `extract_pending_files()` — PDF-OCR im Hintergrund
- `fetch_person_photos()` — Personenbilder herunterladen

### FastAPI (API)

Zwei separate Subapps:

**`/api/oparl/*`** — Aggregierte OParl-API (öffentlich, read-only)
- Folgt OParl-1.1-Spec
- Konsumenten: Mandari, Drittanbieter, Bürger
- Pagination via `?page=N&modified_since=ISO`

**`/api/admin/*`** — Management-API
- Auth: `X-API-Key` Header
- Sources verwalten (CRUD)
- Sync-Jobs triggern
- Logs einsehen
- Statistiken

### PostgreSQL (Datenbank)

**JSONB-First Design**: Jede OParl-Entität speichert ihr vollständiges JSON in `raw`. Indexierte Spalten für schnelle Queries.

**Vorteile**:
- Flexibilität bei OParl-Erweiterungen
- Vollständige Rekonstruktion möglich
- Schnelle Lookups via Indizes

### OParl-Client (`ingestor.oparl.client`)

- httpx async + HTTP/2
- Tenacity Retries (Exponential Backoff)
- Semaphore für Rate-Limiting
- Pagination via `links.next`
- OParl 1.0 + 1.1 kompatibel

## Datenfluss

### Incremental Sync (alle 10 Minuten)

1. Prefect Schedule triggert `sync_all_active_sources`
2. Pro Source: `sync_source_flow(source_id)`
3. Lädt System → Bodies
4. Pro Body parallel:
   - `?modified_since=last_sync` für jeden Endpoint (organizations, persons, ...)
   - Upsert pro Item via `upsert_entity` Task
5. Schreibt SyncLog mit Statistik

### Full Sync (täglich 3 Uhr)

Wie incremental, aber **ohne** `modified_since` → lädt alles neu.

### Mandari-Anbindung (geplant)

Mandari wird in Zukunft die OParl-Daten via dieser API lesen statt
direkt von den Kommune-APIs:

```python
# Django-Service in Mandari
import httpx

INGESTOR_URL = "https://ingestor.mandari.de/api/oparl"

async def fetch_papers(body_id: str, modified_since: str | None = None):
    async with httpx.AsyncClient() as client:
        params = {"modified_since": modified_since} if modified_since else None
        response = await client.get(f"{INGESTOR_URL}/body/{body_id}/papers", params=params)
        return response.json()
```

## Erweiterungspunkte

- **Neue Datenanreicherungen** als zusätzliche Flows (Geo-Coding, KI-Zusammenfassungen)
- **Kafka-Integration** für Echtzeit-Events (z.B. neue Sitzung)
- **Webhooks** an Konsumenten bei Datenupdates
- **OParl-2.0 Support** sobald Spec final
