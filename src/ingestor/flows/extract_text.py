"""
OCR-Worker: PDF-Text-Extraktion mit 3-stufiger Fallback-Chain.

Läuft UNABHÄNGIG vom Sync als eigener Prozess/Container.

Fallback-Chain:
  1. pypdf (lokal, schnell, kostenlos) — funktioniert bei digitalen PDFs
  2. Tesseract OCR (lokal, langsamer, kostenlos) — funktioniert bei gescannten PDFs
  3. KI-API OCR (Mistral/Deepseek, kostenpflichtig) — letzter Versuch
  4. Wenn nichts → ocr_status = "failed"

DB-Felder:
  - ocr_done: false (default) → true (erfolgreich) oder false (failed)
  - text_extraction_status: "pending" → "processing" → "done" / "failed"
  - text_extraction_method: "pypdf" / "tesseract" / "mistral" / "deepseek"
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from uuid import UUID

import httpx
from prefect import flow, get_run_logger
from sqlalchemy import select, update

from ingestor.config import get_settings
from ingestor.db import get_session
from ingestor.db.models import File

logger = logging.getLogger(__name__)

SUPPORTED_MIME_TYPES = {"application/pdf", "text/plain", "text/html", "text/csv"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
MIN_TEXT_LENGTH = 50  # Mindestens 50 Zeichen für "gültigen" Text


# =============================================================================
# Haupt-Task: Download + Extraktion mit Fallback-Chain
# =============================================================================


async def ocr_extract_file(file_id: UUID) -> dict:
    """
    Lädt eine Datei herunter und extrahiert Text.

    Fallback-Chain: pypdf → Tesseract → KI-API (Mistral/Deepseek)

    Returns:
        {"status": "done", "method": "pypdf", "chars": 1234, "pages": 5}
        oder {"status": "failed", "error": "..."}
    """
    # File-Metadaten laden
    async with get_session() as session:
        file_obj = (await session.execute(select(File).where(File.id == file_id))).scalar_one_or_none()
        if not file_obj:
            return {"status": "failed", "error": "File nicht gefunden"}

        url = file_obj.download_url or file_obj.access_url
        if not url:
            await _set_failed(file_id, "Keine Download-URL")
            return {"status": "failed", "error": "Keine URL"}

        mime = file_obj.mime_type or ""

    # Nur PDFs und Text-Dateien verarbeiten
    if mime and mime not in SUPPORTED_MIME_TYPES and not mime.startswith("application/"):
        await _set_failed(file_id, f"MIME-Typ nicht unterstützt: {mime}")
        return {"status": "failed", "error": f"Unsupported: {mime}"}

    # --- Download ---
    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            response = await client.get(url, headers={"User-Agent": "Mandari-Ingestor/0.1"})
            if response.status_code != 200:
                await _set_failed(file_id, f"HTTP {response.status_code}")
                return {"status": "failed", "error": f"HTTP {response.status_code}"}
        data = response.content
    except Exception as exc:
        await _set_failed(file_id, f"Download: {exc}")
        return {"status": "failed", "error": str(exc)}

    if len(data) > MAX_FILE_SIZE:
        await _set_failed(file_id, f"Zu groß: {len(data)} bytes")
        return {"status": "failed", "error": "Too large"}

    sha256 = hashlib.sha256(data).hexdigest()
    is_pdf = data[:5] == b"%PDF-"

    # =========================================================================
    # STUFE 1: pypdf (lokal, schnell, kostenlos)
    # =========================================================================
    if is_pdf:
        text, page_count = _extract_pypdf(data)
        if text and len(text.strip()) >= MIN_TEXT_LENGTH:
            await _save_success(file_id, text.strip(), "pypdf", page_count, sha256)
            return {"status": "done", "method": "pypdf", "chars": len(text.strip()), "pages": page_count}

    # =========================================================================
    # STUFE 2: Tesseract OCR (lokal, langsamer, kostenlos)
    # =========================================================================
    if is_pdf:
        text, page_count = _extract_tesseract(data)
        if text and len(text.strip()) >= MIN_TEXT_LENGTH:
            await _save_success(file_id, text.strip(), "tesseract", page_count, sha256)
            return {"status": "done", "method": "tesseract", "chars": len(text.strip()), "pages": page_count}

    # =========================================================================
    # STUFE 3: KI-API OCR (Mistral oder Deepseek, kostenpflichtig)
    # =========================================================================
    if is_pdf:
        text = await _extract_ki_api(data, url)
        if text and len(text.strip()) >= MIN_TEXT_LENGTH:
            await _save_success(file_id, text.strip(), "ki-api", page_count, sha256)
            return {"status": "done", "method": "ki-api", "chars": len(text.strip()), "pages": None}

    # =========================================================================
    # STUFE 0: Plaintext-Dateien (kein PDF)
    # =========================================================================
    if not is_pdf:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text = data.decode("latin-1")
            except Exception:
                text = ""
        if text and len(text.strip()) >= MIN_TEXT_LENGTH:
            await _save_success(file_id, text.strip(), "plaintext", None, sha256)
            return {"status": "done", "method": "plaintext", "chars": len(text.strip()), "pages": None}

    # =========================================================================
    # FEHLGESCHLAGEN: Keine Methode hat Text extrahiert
    # =========================================================================
    await _set_failed(file_id, "Kein Text extrahierbar (alle Methoden fehlgeschlagen)")
    return {"status": "failed", "error": "Alle Methoden fehlgeschlagen"}


# =============================================================================
# Extraktions-Methoden
# =============================================================================


def _extract_pypdf(data: bytes) -> tuple[str, int | None]:
    """Stufe 1: pypdf — digitale PDFs mit eingebettetem Text."""
    try:
        import io

        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data))
        pages = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            pages.append(page_text)
        text = "\n\n".join(pages)
        return text, len(reader.pages)
    except Exception as exc:
        logger.debug("pypdf failed: %s", exc)
        return "", None


def _extract_tesseract(data: bytes) -> tuple[str, int | None]:
    """Stufe 2: Tesseract OCR — gescannte PDFs als Bilder."""
    try:
        import pytesseract
        from pdf2image import convert_from_bytes

        # Maximal 10 Seiten (Speicher + Zeit begrenzen)
        images = convert_from_bytes(data, dpi=200, first_page=1, last_page=10)
        texts = []
        for img in images:
            text = pytesseract.image_to_string(img, lang="deu+eng")
            texts.append(text)
        full_text = "\n\n".join(texts)
        return full_text, len(images)
    except Exception as exc:
        logger.debug("Tesseract failed: %s", exc)
        return "", None


async def _extract_ki_api(data: bytes, source_url: str) -> str:
    """
    Stufe 3: KI-API OCR via OpenAI-kompatible API.

    Funktioniert mit JEDER OpenAI-kompatiblen API:
    Mistral, Deepseek, OpenAI, Groq, Ollama, vLLM, LiteLLM, etc.

    Konfiguration über .env:
      OCR_AI_API_KEY=sk-...
      OCR_AI_BASE_URL=https://api.mistral.ai/v1
      OCR_AI_MODEL=mistral-ocr-latest

    Ohne OCR_AI_API_KEY: Stufe 3 wird übersprungen (nur pypdf + Tesseract).
    """
    settings = get_settings()

    api_key = settings.ocr_ai_api_key
    if not api_key:
        # Keine KI konfiguriert — überspringe Stufe 3
        return ""

    base_url = settings.ocr_ai_base_url.rstrip("/")
    model = settings.ocr_ai_model

    # Spezialfall: Mistral OCR hat eigenen /ocr Endpoint
    if "mistral" in base_url and "ocr" in model.lower():
        return await _mistral_ocr_endpoint(source_url, api_key, base_url, model)

    # Standard: OpenAI-kompatibles /chat/completions
    return await _openai_compatible_ocr(source_url, api_key, base_url, model)


async def _mistral_ocr_endpoint(pdf_url: str, api_key: str, base_url: str, model: str) -> str:
    """Mistral hat einen eigenen /ocr Endpoint (nicht /chat/completions)."""
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{base_url}/ocr",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": model,
                    "document": {"type": "document_url", "document_url": pdf_url},
                },
            )
            if response.status_code != 200:
                logger.warning("Mistral OCR HTTP %d: %s", response.status_code, response.text[:200])
                return ""
            result = response.json()
            pages = result.get("pages", [])
            texts = [p.get("markdown", "") for p in pages]
            return "\n\n".join(texts)
    except Exception as exc:
        logger.debug("Mistral OCR failed: %s", exc)
        return ""


async def _openai_compatible_ocr(pdf_url: str, api_key: str, base_url: str, model: str) -> str:
    """
    Generischer OpenAI-kompatibler OCR via /chat/completions.

    Funktioniert mit: OpenAI, Deepseek, Groq, Ollama, vLLM, LiteLLM, etc.
    Sendet die PDF-URL als image_url (Vision-API) an das Modell.
    """
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Du bist ein OCR-System. Extrahiere den gesamten Text "
                                "aus dem Dokument. Gib NUR den extrahierten Text zurück, "
                                "keine Kommentare, keine Formatierung, kein Markdown."
                            ),
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Extrahiere den vollständigen Text aus diesem Dokument:",
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {"url": pdf_url},
                                },
                            ],
                        },
                    ],
                    "max_tokens": 8192,
                    "temperature": 0.0,
                },
            )
            if response.status_code != 200:
                logger.warning(
                    "KI-OCR HTTP %d (%s/%s): %s",
                    response.status_code,
                    base_url,
                    model,
                    response.text[:200],
                )
                return ""
            result = response.json()
            choices = result.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "")
            return ""
    except Exception as exc:
        logger.debug("KI-OCR failed (%s/%s): %s", base_url, model, exc)
        return ""


# =============================================================================
# DB-Operationen (atomare UPDATEs, kein ORM read-modify-write)
# =============================================================================


async def _save_success(file_id: UUID, text: str, method: str, page_count: int | None, sha256: str) -> None:
    """Speichert erfolgreich extrahierten Text. OCR = done."""
    async with get_session() as session:
        await session.execute(
            update(File)
            .where(File.id == file_id)
            .values(
                text_content=text,
                text_extraction_status="done",
                text_extraction_method=method,
                text_extraction_error=None,
                page_count=page_count,
                sha256_hash=sha256,
                text_extracted_at=datetime.now(UTC),
            )
        )


async def _set_failed(file_id: UUID, error: str) -> None:
    """Markiert File als fehlgeschlagen. OCR = failed."""
    async with get_session() as session:
        await session.execute(
            update(File)
            .where(File.id == file_id)
            .values(
                text_extraction_status="failed",
                text_extraction_error=error,
                text_extracted_at=datetime.now(UTC),
            )
        )


# =============================================================================
# Flow: Batch-Verarbeitung ausstehender Files
# =============================================================================


@flow(name="ocr-worker", log_prints=True)
async def ocr_worker_flow(batch_size: int = 30, max_concurrent: int = 3) -> dict:
    """
    OCR-Worker-Flow: Verarbeitet ausstehende PDF-Files.

    Läuft als eigenständiger Container, NICHT im Sync-Zyklus.
    Verwendet SELECT FOR UPDATE SKIP LOCKED für parallele Worker.
    """
    import asyncio

    log = get_run_logger()

    # Atomares Claim: pending → processing
    async with get_session() as session:
        claimed = (
            (
                await session.execute(
                    select(File.id)
                    .where(File.text_extraction_status == "pending")
                    .order_by(File.created_at)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        if claimed:
            await session.execute(
                update(File).where(File.id.in_(claimed)).values(text_extraction_status="processing")
            )

    if not claimed:
        log.info("Keine ausstehenden Files für OCR")
        return {"processed": 0, "success": 0, "failed": 0}

    log.info("OCR-Worker: %d Files geclaimt", len(claimed))

    # Parallel verarbeiten
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process(fid: UUID) -> dict:
        async with semaphore:
            return await ocr_extract_file(fid)

    results = await asyncio.gather(*[process(fid) for fid in claimed], return_exceptions=True)

    success = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "done")
    failed = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "failed")
    errors = sum(1 for r in results if isinstance(r, Exception))

    # Methoden-Statistik
    methods: dict[str, int] = {}
    for r in results:
        if isinstance(r, dict) and r.get("method"):
            m = r["method"]
            methods[m] = methods.get(m, 0) + 1

    log.info(
        "OCR-Worker fertig: %d/%d erfolgreich, %d fehlgeschlagen, %d Fehler | Methoden: %s",
        success,
        len(claimed),
        failed,
        errors,
        methods,
    )

    return {
        "processed": len(claimed),
        "success": success,
        "failed": failed,
        "errors": errors,
        "methods": methods,
    }
