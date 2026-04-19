"""
Prefect-Flow: PDF-Text-Extraktion.

Läuft UNABHÄNGIG vom Sync-Flow als eigener Prozess.
Prüft die DB auf Files mit text_extraction_status='pending' und
extrahiert Text via pypdf → Tesseract-OCR Fallback-Chain.

Kann als eigenständiger Worker/Container laufen:
    uv run python -c "from ingestor.flows.extract_text import ...; ..."

Oder via Prefect-Schedule alle 5 Minuten.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from uuid import UUID

import httpx
from prefect import flow, get_run_logger, task
from sqlalchemy import select

from ingestor.db import get_session
from ingestor.db.models import File

logger = logging.getLogger(__name__)

SUPPORTED_MIME_TYPES = {"application/pdf", "text/plain", "text/html", "text/csv"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


@task(name="download-and-extract", retries=1, retry_delay_seconds=10)
async def download_and_extract(file_id: UUID) -> dict:
    """
    Lädt eine Datei herunter und extrahiert den Text.

    Fallback-Chain: pypdf → Tesseract OCR

    Returns: {"chars": int, "method": str, "pages": int|None}
    """
    async with get_session() as session:
        file_obj = (await session.execute(select(File).where(File.id == file_id))).scalar_one_or_none()
        if not file_obj:
            return {"error": "File nicht gefunden"}

        url = file_obj.download_url or file_obj.access_url
        if not url:
            file_obj.text_extraction_status = "skipped"
            file_obj.text_extraction_error = "Keine Download-URL"
            return {"error": "Keine URL"}

        mime = file_obj.mime_type or ""
        if mime and mime not in SUPPORTED_MIME_TYPES and not mime.startswith("application/"):
            file_obj.text_extraction_status = "skipped"
            file_obj.text_extraction_error = f"MIME-Typ nicht unterstützt: {mime}"
            return {"error": f"Unsupported: {mime}"}

    # Download
    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            response = await client.get(url, headers={"User-Agent": "Mandari-Ingestor/0.1"})
            if response.status_code != 200:
                await _update_status(file_id, "failed", f"HTTP {response.status_code}")
                return {"error": f"HTTP {response.status_code}"}

        data = response.content
        if len(data) > MAX_FILE_SIZE:
            await _update_status(file_id, "skipped", f"Zu groß: {len(data)} bytes")
            return {"error": "Too large"}

    except Exception as exc:
        await _update_status(file_id, "failed", f"Download: {exc}")
        return {"error": str(exc)}

    # Extraction
    text = ""
    method = ""
    page_count = None

    # 1. pypdf
    if data[:5] == b"%PDF-":
        text, page_count, method = _extract_pdf(data)

    # 2. Plaintext fallback
    if not text.strip():
        try:
            text = data.decode("utf-8")
            method = "plaintext"
        except UnicodeDecodeError:
            try:
                text = data.decode("latin-1")
                method = "plaintext-latin1"
            except Exception:
                pass

    if text.strip():
        sha256 = hashlib.sha256(data).hexdigest()
        await _save_text(file_id, text.strip(), method, page_count, sha256)
        return {"chars": len(text.strip()), "method": method, "pages": page_count}
    else:
        await _update_status(file_id, "failed", "Kein Text extrahierbar")
        return {"error": "Kein Text"}


def _extract_pdf(data: bytes) -> tuple[str, int | None, str]:
    """Extrahiert Text aus PDF via pypdf. Gibt (text, page_count, method) zurück."""
    try:
        import io

        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data))
        pages = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            pages.append(page_text)

        text = "\n\n".join(pages)
        page_count = len(reader.pages)

        if len(text.strip()) > 50:
            return text, page_count, "pypdf"

        # Fallback: Tesseract OCR
        try:
            text_ocr, success = _ocr_extract(data)
            if success and len(text_ocr.strip()) > len(text.strip()):
                return text_ocr, page_count, "tesseract"
        except Exception:
            pass

        return text, page_count, "pypdf"
    except Exception as exc:
        logger.warning("pypdf failed: %s", exc)
        return "", None, ""


def _ocr_extract(data: bytes) -> tuple[str, bool]:
    """OCR via Tesseract (pdf2image + pytesseract)."""
    try:
        import pytesseract
        from pdf2image import convert_from_bytes

        images = convert_from_bytes(data, dpi=200, first_page=1, last_page=5)
        texts = []
        for img in images:
            text = pytesseract.image_to_string(img, lang="deu")
            texts.append(text)
        full_text = "\n\n".join(texts)
        return full_text, len(full_text.strip()) > 50
    except Exception as exc:
        logger.warning("OCR failed: %s", exc)
        return "", False


async def _update_status(file_id: UUID, status: str, error: str | None = None) -> None:
    async with get_session() as session:
        f = (await session.execute(select(File).where(File.id == file_id))).scalar_one_or_none()
        if f:
            f.text_extraction_status = status
            f.text_extraction_error = error
            f.text_extracted_at = datetime.now(UTC)


async def _save_text(file_id: UUID, text: str, method: str, page_count: int | None, sha256: str) -> None:
    async with get_session() as session:
        f = (await session.execute(select(File).where(File.id == file_id))).scalar_one_or_none()
        if f:
            f.text_content = text
            f.text_extraction_status = "done"
            f.text_extraction_method = method
            f.page_count = page_count
            f.sha256_hash = sha256
            f.text_extracted_at = datetime.now(UTC)


@flow(name="extract-pending-texts", log_prints=True)
async def extract_pending_texts(batch_size: int = 50, max_concurrent: int = 3) -> dict:
    """
    Verarbeitet ausstehende PDF-Files.

    Läuft als eigenständiger Flow, NICHT im Sync-Zyklus.
    Kann als Worker/Container laufen oder per Cron alle 5 Min.
    """
    import asyncio

    log = get_run_logger()

    async with get_session() as session:
        pending = (
            (
                await session.execute(
                    select(File.id)
                    .where(File.text_extraction_status == "pending")
                    .order_by(File.created_at)
                    .limit(batch_size)
                )
            )
            .scalars()
            .all()
        )

    if not pending:
        log.info("No pending files for extraction")
        return {"processed": 0}

    log.info("Extracting text from %d files", len(pending))

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process(fid: UUID) -> dict:
        async with semaphore:
            return await download_and_extract(fid)

    results = await asyncio.gather(*[process(fid) for fid in pending], return_exceptions=True)

    success = sum(1 for r in results if isinstance(r, dict) and "chars" in r)
    failed = sum(1 for r in results if isinstance(r, dict) and "error" in r)
    errors = sum(1 for r in results if isinstance(r, Exception))

    log.info("Extraction done: %d success, %d failed, %d errors", success, failed, errors)
    return {"processed": len(pending), "success": success, "failed": failed, "errors": errors}
