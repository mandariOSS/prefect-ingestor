"""
Application Settings via pydantic-settings.

Werden aus .env oder Environment-Variablen geladen.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Anwendungskonfiguration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # === Datenbank ===
    database_url: str = Field(
        default="postgresql+asyncpg://ingestor:ingestor_dev@localhost:5432/ingestor",
        description="Async DB-URL (asyncpg)",
    )

    @property
    def database_url_sync(self) -> str:
        """Sync-URL für Alembic (psycopg)."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")

    # === Prefect ===
    prefect_api_url: str = "http://localhost:4200/api"
    prefect_api_key: str | None = None

    # === API ===
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    api_key: str = Field(
        default="change-me",
        description="Bootstrap-API-Key für Management-API (nur wenn keine Keys in DB)",
    )
    # Erlaubt Bootstrap-Betrieb mit Default-Key; in Prod MUSS False sein
    dev_mode: bool = Field(
        default=False,
        description="Erlaubt Default-API-Key. NIEMALS in Produktion auf True.",
    )
    # Admin-CORS: strikte Liste, OParl bleibt offen (Spec verlangt *)
    admin_cors_origins: str = Field(
        default="http://localhost:8000",
        description="Kommagetrennte Origins für /api/admin (Credentialed-CORS)",
    )
    # Rate-Limiting
    rate_limit_default: str = "60/minute"
    rate_limit_auth_failure: str = "5/minute"

    # === OParl-Sync ===
    sync_interval_minutes: int = 10
    full_sync_hour: int = 3
    http_timeout_seconds: int = 60
    http_max_retries: int = 3
    http_user_agent: str = "Mandari-Ingestor/0.1 (https://mandari.de)"
    # Maximale parallele Bodies pro Sync-Zyklus (begrenzt DB- und HTTP-Last).
    sync_body_concurrency: int = 4
    # Nach N aufeinander folgenden Fehlschlägen wird die Quelle automatisch
    # in Quarantäne gesetzt (is_active=False, quarantined_at=now).
    sync_quarantine_threshold: int = 10
    # Timeout für den Sync einer einzelnen Quelle (verhindert "stuck sync").
    sync_source_timeout_seconds: int = 1800  # 30 Min
    # Enrichment nach Body-Sync automatisch triggern
    enrichment_auto_photos: bool = True
    # DEPRECATED seit v0.2: ignoriert. Geocoding läuft im dedizierten
    # GeocodingWorker (workers/geocoding.py), unabhängig vom Sync-Flow.
    # Setting bleibt nur für Backwards-Compat mit alten .env-Files erhalten.
    enrichment_auto_geocoding: bool = False

    # === OCR-Worker ===
    ocr_enabled: bool = True
    ocr_batch_size: int = 30
    ocr_concurrency: int = 3
    ocr_max_file_size_mb: int = 50

    # === KI-API OCR (Stufe 3 Fallback, optional) ===
    # Jede OpenAI-kompatible API: Mistral, Deepseek, OpenAI, Groq, Ollama, etc.
    # Ohne diese Keys läuft der OCR-Worker nur mit pypdf + Tesseract (kostenlos).
    ocr_ai_api_key: str | None = None
    ocr_ai_base_url: str = "https://api.mistral.ai/v1"  # OpenAI-kompatibel
    ocr_ai_model: str = "mistral-ocr-latest"
    # Beispiele:
    #   Mistral:  base_url=https://api.mistral.ai/v1  model=mistral-ocr-latest
    #   OpenAI:   base_url=https://api.openai.com/v1   model=gpt-4o
    #   Deepseek: base_url=https://api.deepseek.com    model=deepseek-chat
    #   Groq:     base_url=https://api.groq.com/openai/v1  model=llama-3.3-70b-versatile
    #   Ollama:   base_url=http://localhost:11434/v1    model=llama3.2-vision

    # === Geocoding (Nominatim) ===
    # Default: öffentliche OSM-Nominatim-Instanz. Eigenbetrieb möglich, aber für
    # die typische Mandari-Last reichen die Public-Limits (1 req/s) bei weitem,
    # solange der GeocodingWorker (siehe workers/geocoding.py) die Anfragen
    # serialisiert und mit GEOCODING_INTERVAL_SECONDS pausiert.
    nominatim_url: str = "https://nominatim.openstreetmap.org"
    # Pflichtfeld nach Nominatim-Usage-Policy: identifiziert die Anwendung +
    # liefert Kontakt im Missbrauchsfall. Niemals leer lassen.
    nominatim_user_agent: str = "Mandari-Ingestor/0.1 (kontakt@mandari.de)"
    # Email als zusätzlicher Identifier (Nominatim akzeptiert sowohl Header
    # als auch ?email=-Parameter; setzen wir beides, um robust zu sein).
    nominatim_email: str = "kontakt@mandari.de"

    # === Geocoding-Worker (dedicated, rate-limited) ===
    # Eigener Container, der seriell Locations ohne Koordinaten geocodet.
    # Auf False setzen, wenn Geocoding manuell oder über externes Tool laufen soll.
    geocoding_enabled: bool = True
    # Sekunden zwischen zwei Geocoding-Anfragen. Default 5 s ist konservativ
    # — Public-Nominatim erlaubt 1/s, aber 5 s lässt deutlich Luft für andere
    # Mandari-Komponenten und sehr großzügige Burst-Tolerance.
    geocoding_interval_seconds: float = 5.0
    # Max. Locations pro Worker-Iteration (kleiner Batch hält Memory niedrig
    # und gibt schnelle Reaktion auf neue Daten zwischen den Sleeps).
    geocoding_batch_size: int = 50
    # Pause wenn keine Arbeit ansteht (Worker bleibt aktiv, prüft regelmäßig)
    geocoding_idle_sleep_seconds: int = 60

    # === Logging ===
    log_level: str = "INFO"
    log_format: str = "json"  # oder "console"

    # === CORS ===
    cors_origins: str = "http://localhost:8000"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def admin_cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.admin_cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    """Cached Settings-Instanz (lazy)."""
    return Settings()
