# =============================================================================
# Mandari Prefect Ingestor — Multi-Stage Dockerfile
# =============================================================================

# --- Stage 1: Build dependencies via uv ---
FROM python:3.12-slim AS builder

# uv installieren (schneller als pip)
COPY --from=ghcr.io/astral-sh/uv:0.4.30 /uv /usr/local/bin/uv

WORKDIR /app

# Build-Tools für native Wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Dependencies installieren
COPY pyproject.toml README.md ./
COPY src/ src/
RUN uv pip install --system --no-cache .

# --- Stage 2: Runtime ---
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/usr/local/bin:$PATH

WORKDIR /app

# Runtime-Abhängigkeiten (libpq, OCR-Tools, curl für Healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-deu \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root User
RUN useradd --create-home --shell /bin/bash ingestor

# Python-Pakete aus Builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Application-Code
COPY --chown=ingestor:ingestor src/ ./src/
COPY --chown=ingestor:ingestor alembic.ini ./

USER ingestor

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# Default: API starten
CMD ["uvicorn", "ingestor.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
