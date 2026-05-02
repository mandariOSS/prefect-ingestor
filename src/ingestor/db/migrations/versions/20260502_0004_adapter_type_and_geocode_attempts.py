"""Adapter-Framework: Source.adapter_type + Location.geocode_attempts.

Diese Migration unterstützt zwei neue Features:

1.  **Adapter-Framework** (Source.adapter_type)
    Bisher waren alle Sources OParl. Mit dem neuen Adapter-Framework können
    Sources auch HTML-Scraper-Adapter nutzen (SessionNet, ALLRIS, SD.NET).
    Default für alle bestehenden Rows: ``"oparl"`` (volle Backwards-Compat).

2.  **Geocoding-Worker** (Location.geocode_attempts)
    Der dedizierte GeocodingWorker zählt pro Location, wie oft schon ein
    Geocoding-Versuch gemacht wurde. Ab 3 Misses (MAX_GEOCODE_ATTEMPTS in
    workers/geocoding.py) wird die Location nicht mehr versucht. Default 0.

Plus ein Partial-Index für die Worker-Claim-Query (latitude IS NULL AND
street_address IS NOT NULL — der Hot-Path für jeden Worker-Tick).

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ── 1. Source.adapter_type ─────────────────────────────────────────────
    # Default 'oparl' für alle bestehenden Rows (volle Backwards-Compat).
    op.add_column(
        "sources",
        sa.Column(
            "adapter_type",
            sa.String(40),
            nullable=False,
            server_default="oparl",
        ),
    )
    op.create_index(
        "ix_sources_adapter_type",
        "sources",
        ["adapter_type"],
    )

    # ── 2. Location.geocode_attempts ──────────────────────────────────────
    # Default 0. Worker inkrementiert bei Hit + Miss.
    op.add_column(
        "locations",
        sa.Column(
            "geocode_attempts",
            sa.Integer,
            nullable=True,
            server_default="0",
        ),
    )

    # Partial-Index für die Worker-Claim-Query. Erfasst nur die Rows, die
    # tatsächlich Geocoding brauchen — bei großer Location-Tabelle deutlich
    # effizienter als ein Full-Index. Postgres-only.
    op.create_index(
        "ix_location_geocode_pending",
        "locations",
        ["geocode_attempts"],
        postgresql_where=sa.text(
            "latitude IS NULL AND street_address IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_location_geocode_pending", table_name="locations")
    op.drop_column("locations", "geocode_attempts")
    op.drop_index("ix_sources_adapter_type", table_name="sources")
    op.drop_column("sources", "adapter_type")
