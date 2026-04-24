"""
OCR-Worker-Entrypoint: Loop, der alle N Sekunden ocr_worker_flow ausführt.

Ersetzt den inline Python-Code aus der docker-compose.yml — eigenes Modul
ist testbar, editierbar und besser wartbar.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime

# Prefect ohne Server
os.environ.setdefault("PREFECT_API_URL", "")

from ingestor.config import get_settings  # noqa: E402
from ingestor.flows.extract_text import ocr_worker_flow  # noqa: E402

logger = logging.getLogger(__name__)

# Intervall zwischen Batches in Sekunden
OCR_IDLE_SLEEP = 300


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


async def run_forever() -> None:
    settings = get_settings()
    _setup_logging(settings.log_level)

    logger.info(
        "OCR-Worker started — batch=%d, concurrency=%d, idle_sleep=%ds",
        settings.ocr_batch_size,
        settings.ocr_concurrency,
        OCR_IDLE_SLEEP,
    )

    stop_event = asyncio.Event()

    def _handler() -> None:
        logger.info("OCR-Worker received shutdown signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:
            pass

    while not stop_event.is_set():
        started = datetime.now(UTC)
        try:
            result = await ocr_worker_flow(
                batch_size=settings.ocr_batch_size,
                max_concurrent=settings.ocr_concurrency,
            )
            duration = (datetime.now(UTC) - started).total_seconds()
            logger.info("OCR-Batch in %.1fs: %s", duration, result)
        except Exception:
            logger.exception("OCR-Worker cycle failed — continuing")

        # Idle-sleep, aber Shutdown-signal aufwachen lassen
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=OCR_IDLE_SLEEP)
        except TimeoutError:
            continue

    logger.info("OCR-Worker stopped")


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
