"""Reliability: Source quarantine columns + performance indexes.

- sources.consecutive_failures (int, default 0)
- sources.quarantined_at (timestamp, nullable)
- Composite indexes (body_id, oparl_modified) für Incremental-Queries auf
  organizations, persons, meetings, papers, files, locations, legislative_terms
- Composite-Index (text_extraction_status, created_at) auf files für den
  OCR-Claim-Query (SELECT ... FOR UPDATE SKIP LOCKED)
- Composite-Index (body_id, date) auf papers

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sources",
        sa.Column("consecutive_failures", sa.Integer, nullable=False, server_default="0"),
    )
    op.add_column(
        "sources",
        sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_org_body_modified", "organizations", ["body_id", "oparl_modified"])
    op.create_index("ix_person_body_modified", "persons", ["body_id", "oparl_modified"])
    op.create_index("ix_term_body_modified", "legislative_terms", ["body_id", "oparl_modified"])
    op.create_index("ix_meeting_body_modified", "meetings", ["body_id", "oparl_modified"])
    op.create_index("ix_paper_body_modified", "papers", ["body_id", "oparl_modified"])
    op.create_index("ix_paper_body_date", "papers", ["body_id", "date"])
    op.create_index("ix_file_body_modified", "files", ["body_id", "oparl_modified"])
    op.create_index("ix_file_ocr_claim", "files", ["text_extraction_status", "created_at"])
    op.create_index("ix_location_body_modified", "locations", ["body_id", "oparl_modified"])


def downgrade() -> None:
    op.drop_index("ix_location_body_modified", table_name="locations")
    op.drop_index("ix_file_ocr_claim", table_name="files")
    op.drop_index("ix_file_body_modified", table_name="files")
    op.drop_index("ix_paper_body_date", table_name="papers")
    op.drop_index("ix_paper_body_modified", table_name="papers")
    op.drop_index("ix_meeting_body_modified", table_name="meetings")
    op.drop_index("ix_term_body_modified", table_name="legislative_terms")
    op.drop_index("ix_person_body_modified", table_name="persons")
    op.drop_index("ix_org_body_modified", table_name="organizations")

    op.drop_column("sources", "quarantined_at")
    op.drop_column("sources", "consecutive_failures")
