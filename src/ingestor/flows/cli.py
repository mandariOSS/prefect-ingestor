"""CLI für manuelle Flow-Ausführung (typer)."""

import asyncio
from uuid import UUID

import typer

app = typer.Typer(help="Mandari Ingestor — Manueller Flow-Trigger")


@app.command()
def sync(source_id: str = typer.Argument(..., help="Source UUID"), full: bool = False) -> None:
    """Führt einen sync_source_flow synchron aus."""
    from ingestor.flows.sync_source import sync_source_flow

    result = asyncio.run(sync_source_flow(UUID(source_id), full=full))
    typer.echo(result)


@app.command()
def sync_all(full: bool = False) -> None:
    """Synchronisiert alle aktiven Quellen."""
    from ingestor.flows.sync_source import sync_all_active_sources

    asyncio.run(sync_all_active_sources(full=full))


if __name__ == "__main__":
    app()
