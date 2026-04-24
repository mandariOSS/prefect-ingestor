"""
Scheduler-Worker: triggert den Sync in einem einfachen Event-Loop ohne Prefect-Server.

Pattern (identisch zum OCR-Worker): Container startet, loop prüft alle N Minuten
die DB und ruft sync_all_active_sources auf. Täglich um FULL_SYNC_HOUR Uhr läuft
statt incremental ein Full-Sync.

Robustheit:
- Fehler in einer Iteration brechen den Loop nicht — nur diese eine Runde ist
  verloren, nächste Iteration läuft normal.
- Beim Start wird einmal der Source-Seed versucht (no-op wenn DB gefüllt).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime, timedelta

# Prefect ohne Server: Flows laufen inline
os.environ.setdefault("PREFECT_API_URL", "")

from ingestor.config import get_settings  # noqa: E402
from ingestor.flows.sync_source import sync_all_active_sources  # noqa: E402
from ingestor.workers.seed import seed_sources_if_empty  # noqa: E402

logger = logging.getLogger(__name__)


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


async def _should_run_full(last_full: datetime | None, full_hour: int) -> bool:
    """True wenn heute zwischen full_hour und full_hour+1 noch kein Full-Sync lief."""
    now = datetime.now(UTC)
    if now.hour != full_hour:
        return False
    if last_full is None:
        return True
    # Gleiches Tagesdatum UTC → schon gelaufen
    return last_full.date() != now.date()


async def _run_once(full: bool) -> None:
    sync_type = "full" if full else "incremental"
    started = datetime.now(UTC)
    try:
        results = await sync_all_active_sources(full=full)
        successes = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        failures = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "failed")
        duration = (datetime.now(UTC) - started).total_seconds()
        logger.info(
            "Scheduler: %s sync done in %.1fs — %d success, %d failed, %d total",
            sync_type,
            duration,
            successes,
            failures,
            len(results),
        )
    except Exception:
        logger.exception("Scheduler: %s sync crashed", sync_type)


async def run_forever() -> None:
    settings = get_settings()
    _setup_logging(settings.log_level)

    # Einmalig: Seed versuchen
    try:
        seed_result = await seed_sources_if_empty()
        logger.info("Seed result: %s", seed_result)
    except Exception:
        logger.exception("Seed failed — continuing without seed")

    interval = timedelta(minutes=settings.sync_interval_minutes)
    last_full: datetime | None = None
    logger.info(
        "Scheduler started — interval=%d min, full-sync at %02d:00 UTC",
        settings.sync_interval_minutes,
        settings.full_sync_hour,
    )

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Scheduler received shutdown signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows hat keine Unix-Signals im Event-Loop
            pass

    while not stop_event.is_set():
        cycle_start = datetime.now(UTC)

        if await _should_run_full(last_full, settings.full_sync_hour):
            await _run_once(full=True)
            last_full = cycle_start
        else:
            await _run_once(full=False)

        # Bis zur nächsten geplanten Iteration schlafen, aber aufwachen bei Shutdown
        next_run = cycle_start + interval
        sleep_seconds = max(1.0, (next_run - datetime.now(UTC)).total_seconds())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds)
        except TimeoutError:
            continue

    logger.info("Scheduler stopped")


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
