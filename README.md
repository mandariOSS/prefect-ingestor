# Mandari Prefect Ingestor

Zentraler **OParl-Aggregator** für deutsche Kommunen.
Liest OParl 1.0/1.1-Schnittstellen ein, reichert die Daten an und stellt sie als **eine** OParl-1.1-konforme API für alle Kommunen bereit.

---

## Was es macht

```
118 OParl-Quellen (Kommunen)          →   1 API
├─ Köln, Münster, Bonn, Aachen, ...       /api/oparl/system
├─ Verschiedene RIS-Systeme               /api/oparl/bodies
└─ OParl 1.0 + 1.1 gemischt              /api/oparl/body/{id}/meeting
                                          /api/oparl/body/{id}/paper
                                          ...
```

1. **Liest OParl-Schnittstellen** parallel ein (httpx async, OParl 1.0 + 1.1)
2. **Speichert die Originaldaten** 1:1 in PostgreSQL (JSONB)
3. **Reichert an** (unabhängig vom Sync):
   - PDF-Volltextextraktion (pypdf → Tesseract → KI-API Fallback)
   - Personenbilder (RIS-URL-Pattern-Erkennung)
   - Geo-Koordinaten (lokale Nominatim-Instanz)
4. **Gibt sie weiter** als OParl-1.1-konforme API — Original-Daten, nur `id`-URLs umgeschrieben
5. **Management-API** zum Verwalten von Quellen, Logs und Statistiken

---

## Architektur

```
┌─────────────────────────────────────────────────────┐
│  OParl-APIs deutscher Kommunen (118 Quellen)        │
└──────────────────┬──────────────────────────────────┘
                   │ httpx async (parallel)
                   ▼
┌─────────────────────────────────────────────────────┐
│  Sync-Flow (Prefect)                                │
│  ├─ Incremental: alle 10 Min (modified_since)       │
│  └─ Full: täglich 3 Uhr                             │
└──────────────────┬──────────────────────────────────┘
                   │ SQLAlchemy 2.0 async
                   ▼
┌─────────────────────────────────────────────────────┐
│  PostgreSQL 16 (JSONB + relationale Indizes)        │
│  Original-Daten 1:1 gespeichert                     │
└────────┬─────────────────────────────┬──────────────┘
         │                             │
    ┌────▼─────┐                  ┌────▼──────┐
    │ FastAPI  │                  │OCR-Worker │
    │ OParl API│                  │(unabhängig)│
    │ :8080    │                  │pypdf→Tess.│
    └──────────┘                  │→KI-API    │
                                  └───────────┘
```

---

## Tech-Stack

| Komponente | Technologie |
|------------|-------------|
| Orchestrierung | [Prefect 3](https://prefect.io) |
| API | [FastAPI](https://fastapi.tiangolo.com) |
| HTTP-Client | [httpx](https://www.python-httpx.org) (async, HTTP/2) |
| Datenbank | PostgreSQL 16 (JSONB) |
| ORM | SQLAlchemy 2.0 + asyncpg |
| Migrations | Alembic |
| OCR | pypdf → Tesseract → OpenAI-kompatible KI-API |
| Geocoding | Nominatim (lokale DE-Instanz, optional) |
| Package-Manager | [uv](https://docs.astral.sh/uv) |
| CI/CD | GitHub Actions → ghcr.io |
| Lizenz | AGPL-3.0-or-later |

---

## Quickstart

### Mit Docker (empfohlen)

```bash
git clone https://github.com/mandariOSS/prefect-ingestor.git
cd prefect-ingestor
cp .env.example .env

# Starten (API + Postgres + OCR-Worker)
docker compose up -d

# Erste Quelle hinzufügen
curl -X POST http://localhost:8080/api/admin/sources \
  -H "X-API-Key: dev-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"Stadt Münster","system_url":"https://oparl.stadt-muenster.de/system","is_active":true}'

# Sync auslösen
curl -X POST http://localhost:8080/api/admin/sources/<id>/sync \
  -H "X-API-Key: dev-key" \
  -d '{"full":true}'

# Daten abfragen (OParl 1.1)
curl http://localhost:8080/api/oparl/system
curl http://localhost:8080/api/oparl/bodies
```

### Lokal entwickeln

```bash
uv sync
cp .env.example .env

# Postgres starten (Docker oder lokal)
docker run -d --name ingestor-postgres \
  -e POSTGRES_USER=ingestor -e POSTGRES_PASSWORD=ingestor_dev \
  -e POSTGRES_DB=ingestor -p 5433:5432 postgres:16-alpine

# DB-Schema erstellen
uv run alembic upgrade head

# API starten
uv run uvicorn ingestor.api.main:app --reload --port 8080

# Sync manuell ausführen
PREFECT_API_URL="" uv run python -c "
import asyncio
from ingestor.flows.sync_source import sync_all_active_sources
asyncio.run(sync_all_active_sources(full=True))
"
```

---

## API-Endpoints

### OParl-API (öffentlich, OParl 1.1 spec-konform)

| Endpoint | Beschreibung |
|----------|-------------|
| `GET /api/oparl/system` | System-Einstiegspunkt |
| `GET /api/oparl/bodies` | Alle Kommunen (Bodies) |
| `GET /api/oparl/body/{id}` | Einzelner Body |
| `GET /api/oparl/body/{id}/organization` | Gremien/Fraktionen |
| `GET /api/oparl/body/{id}/person` | Personen |
| `GET /api/oparl/body/{id}/meeting` | Sitzungen |
| `GET /api/oparl/body/{id}/paper` | Vorlagen/Drucksachen |
| `GET /api/oparl/body/{id}/agendaItem` | Tagesordnungspunkte |
| `GET /api/oparl/body/{id}/consultation` | Beratungen |
| `GET /api/oparl/body/{id}/file` | Dateien |
| `GET /api/oparl/body/{id}/membership` | Mitgliedschaften |
| `GET /api/oparl/body/{id}/location` | Orte |
| `GET /api/oparl/body/{id}/legislativeTerm` | Wahlperioden |
| `GET /api/oparl/{type}/{id}` | Detail-Abruf pro Entität |
| `GET /api/oparl/person/{id}/photo` | Personenbild (Mandari-Erweiterung) |

**Query-Parameter** (OParl-Standard):
- `?page=1` — Pagination
- `?modified_since=2024-01-01T00:00:00+01:00` — Incremental Update

**Mandari-Erweiterungen** (Vendor-Prefix `mandari:`):
- `mandari:photoUrl` — Personenbild-URL (auf Person-Objekt)
- `mandari:textExtractionStatus` — OCR-Status (auf File-Objekt)
- `text` — Extrahierter Volltext (Standard-OParl-Feld auf File)
- `geojson` — Geo-Koordinaten (Standard-OParl-Feld auf Location)

### Management-API (Auth: `X-API-Key` Header)

| Endpoint | Beschreibung |
|----------|-------------|
| `GET /api/admin/sources` | Alle Quellen auflisten |
| `POST /api/admin/sources` | Quelle hinzufügen |
| `PATCH /api/admin/sources/{id}` | Quelle bearbeiten |
| `DELETE /api/admin/sources/{id}` | Quelle löschen |
| `POST /api/admin/sources/{id}/sync` | Sync auslösen |
| `GET /api/admin/logs` | Sync-Protokolle |
| `GET /api/admin/stats` | Statistiken |
| `GET /health` | Health-Check |

**Interaktive API-Dokumentation**: http://localhost:8080/docs (Swagger UI)

---

## OCR-Worker

Eigenständiger Service — läuft unabhängig vom Sync.

```
PDF-Datei (status: pending)
  │
  ├─ Stufe 1: pypdf (lokal, kostenlos, ~0.1s)
  │     ✅ Text gefunden → done, method=pypdf
  │
  ├─ Stufe 2: Tesseract OCR (lokal, kostenlos, ~5-30s)
  │     ✅ Text gefunden → done, method=tesseract
  │
  ├─ Stufe 3: KI-API (optional, kostenpflichtig)
  │     ✅ Text gefunden → done, method=ki-api
  │
  └─ ❌ Nichts gefunden → status=failed
```

### KI-API konfigurieren (optional)

Jede **OpenAI-kompatible** API funktioniert:

```env
# Mistral (empfohlen für OCR)
OCR_AI_API_KEY=sk-...
OCR_AI_BASE_URL=https://api.mistral.ai/v1
OCR_AI_MODEL=mistral-ocr-latest

# OpenAI
OCR_AI_BASE_URL=https://api.openai.com/v1
OCR_AI_MODEL=gpt-4o

# Ollama (lokal, kostenlos)
OCR_AI_BASE_URL=http://localhost:11434/v1
OCR_AI_MODEL=llama3.2-vision
OCR_AI_API_KEY=ollama
```

**Ohne KI-API-Key**: Läuft komplett ohne KI — nur pypdf + Tesseract (kostenlos).

---

## Datenintegrität

Sync, OCR und Geocoding laufen **parallel und unabhängig**:

| Mechanismus | Schutz |
|-------------|--------|
| Spalten-Trennung | Sync schreibt OParl-Felder, OCR schreibt Text-Felder — kein Overlap |
| ENRICHMENT_FIELDS Blacklist | Sync überschreibt OCR/Geo/Photo-Ergebnisse nie |
| SELECT FOR UPDATE SKIP LOCKED | Mehrere OCR-Worker bearbeiten nie dieselbe Datei |
| Atomare SQL-UPDATEs | Kein ORM read-modify-write für Enrichment |

---

## Docker-Compose Services

```yaml
docker compose up -d                      # Standard: API + Postgres + OCR-Worker
docker compose --profile geocoding up -d  # + Nominatim (lokales Geocoding)
```

| Service | Port | Beschreibung |
|---------|------|-------------|
| `api` | 8080 | FastAPI (OParl + Management) |
| `postgres` | 5432 | PostgreSQL 16 |
| `ocr-worker` | — | PDF-Textextraktion (Hintergrund) |
| `nominatim` | 8088 | Geocoding DE-only (optional, ~3GB) |

---

## Projektstruktur

```
src/ingestor/
├── config.py                   # pydantic-settings
├── db/
│   ├── models.py               # 12 OParl-Entitäten + Source/SyncLog
│   ├── session.py              # Async Session-Factory
│   └── migrations/             # Alembic
├── oparl/
│   ├── client.py               # Async httpx (HTTP/2, Retries, Pagination)
│   └── types.py                # OParl 1.0/1.1 Typ-Erkennung
├── flows/
│   ├── sync_source.py          # Sync-Flow (alle Quellen parallel)
│   ├── extract_text.py         # OCR-Worker (3-Stufen Fallback)
│   └── tasks/
│       ├── upsert.py           # Generischer OParl-Upsert
│       ├── extract_embedded.py # OParl 1.0 inline-Objekte
│       ├── extract_files.py    # File-URLs aus Papers
│       ├── person_photos.py    # Personenbilder
│       └── geocoding.py        # Nominatim-Geocoding
└── api/
    ├── main.py                 # FastAPI App
    ├── oparl/router.py         # OParl-1.1-API (raw-JSON durchreichen)
    └── management/router.py    # CRUD Sources, Logs, Stats
```

---

## Verwandte Projekte

- **[mandari](https://github.com/mandariOSS/mandari)** — Hauptanwendung (Insight + Work Portal)

## Lizenz

[AGPL-3.0-or-later](LICENSE)
