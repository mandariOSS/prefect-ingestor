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
        description="Auth-Key für Management-API",
    )

    # === OParl-Sync ===
    sync_interval_minutes: int = 10
    full_sync_hour: int = 3
    http_timeout_seconds: int = 60
    http_max_retries: int = 3
    http_user_agent: str = "Mandari-Ingestor/0.1 (https://mandari.de)"

    # === OCR-Worker ===
    ocr_enabled: bool = True
    ocr_batch_size: int = 30
    ocr_concurrency: int = 3
    ocr_max_file_size_mb: int = 50

    # === KI-API OCR (Stufe 3 Fallback, optional) ===
    mistral_api_key: str | None = None
    deepseek_api_key: str | None = None

    # === Logging ===
    log_level: str = "INFO"
    log_format: str = "json"  # oder "console"

    # === CORS ===
    cors_origins: str = "http://localhost:8000"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    """Cached Settings-Instanz (lazy)."""
    return Settings()
