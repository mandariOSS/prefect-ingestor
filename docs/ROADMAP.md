# Roadmap

## Phase 1 — Skelett (Initial Push) ✅

- [x] Repo-Struktur, pyproject, ruff/mypy
- [x] DB-Modelle für alle OParl-Entitäten
- [x] Async OParl-Client (httpx + tenacity)
- [x] Prefect-Flows: `sync_source_flow`, `sync_all_active_sources`
- [x] Generischer Upsert-Task
- [x] FastAPI: Aggregierte OParl-API (Bodies, Organizations, Persons, Meetings, Papers, Files)
- [x] FastAPI: Management-API (Sources CRUD, Logs, Trigger)
- [x] Docker + docker-compose Dev-Setup
- [x] GitHub Actions CI + Release-Build

## Phase 2 — Production-ready 🚧

- [ ] Alembic-Initial-Migration
- [ ] Tests: Unit + Integration (httpx-mock + Postgres)
- [ ] Pre-built OParl-Source-Liste (CSV/Seed-Migration)
- [ ] Strukturiertes Logging mit `structlog`
- [ ] Prometheus-Metriken (Flow-Dauer, Entities-pro-Sync, HTTP-Errors)
- [ ] Health-Endpoint mit DB-Check
- [ ] Rate-Limiting auf API-Endpoints
- [ ] OpenAPI-Schema-Stabilität sichern

## Phase 3 — Datenanreicherung 📋

- [ ] PDF-Text-Extraktion als eigener Flow (`extract_text_flow`)
- [ ] OCR-Fallback (Tesseract) für gescannte PDFs
- [ ] Mistral OCR als optionaler Backend
- [ ] Personenbilder herunterladen (`fetch_person_photos`)
- [ ] Geo-Anreicherung von Vorlagen-Standorten (Nominatim)
- [ ] KI-Zusammenfassungen pro Paper (optional)

## Phase 4 — Mandari-Integration 📋

- [ ] Mandari (Django) liest via diese API statt direkt von OParl
- [ ] Migration-Guide: Mandari-DB → Ingestor-API
- [ ] Webhook-Support für Push-Notifications
- [ ] OAuth2-Flow für Multi-Tenant-Zugriff (statt API-Key)

## Phase 5 — Erweiterungen 📋

- [ ] OParl-2.0 Support (sobald Spec final)
- [ ] Kafka-Stream für Echtzeit-Events
- [ ] GraphQL-API (komplementär zu REST)
- [ ] Public Dashboard (Stats, Coverage-Map)
- [ ] Multi-Region-Deployment
