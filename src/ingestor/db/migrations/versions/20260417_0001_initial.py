"""Initial schema — alle OParl-Entitäten + Management-Tabellen.

Revision ID: 0001
Create Date: 2026-04-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Sources
    op.create_table(
        "sources",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("system_url", sa.Text, nullable=False, unique=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("sync_interval_min", sa.Integer, nullable=False, server_default="10"),
        sa.Column("last_sync_at", sa.DateTime(timezone=True)),
        sa.Column("last_full_sync_at", sa.DateTime(timezone=True)),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text),
        sa.Column("config", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # Bodies
    op.create_table(
        "bodies",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("short_name", sa.String(100)),
        sa.Column("website", sa.Text),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("source_id", "external_id", name="uq_body_source_external"),
    )
    op.create_index("ix_bodies_source_id", "bodies", ["source_id"])
    op.create_index("ix_bodies_external_id", "bodies", ["external_id"])

    # Organizations
    op.create_table(
        "organizations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("short_name", sa.String(100)),
        sa.Column("organization_type", sa.String(100)),
        sa.Column("classification", sa.String(100)),
        sa.Column("start_date", sa.Date),
        sa.Column("end_date", sa.Date),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("body_id", "external_id", name="uq_org_body_external"),
    )
    op.create_index("ix_organizations_body_id", "organizations", ["body_id"])

    # Persons
    op.create_table(
        "persons",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("family_name", sa.String(200)),
        sa.Column("given_name", sa.String(200)),
        sa.Column("title", sa.String(100)),
        sa.Column("email", sa.String(255)),
        sa.Column("phone", sa.String(100)),
        sa.Column("photo_url", sa.Text),
        sa.Column("photo_downloaded_at", sa.DateTime(timezone=True)),
        sa.Column("photo_mime_type", sa.String(100)),
        sa.Column("photo_data", sa.LargeBinary),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("body_id", "external_id", name="uq_person_body_external"),
    )
    op.create_index("ix_persons_body_id", "persons", ["body_id"])

    # Memberships
    op.create_table(
        "memberships",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "person_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("persons.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("role", sa.String(255)),
        sa.Column("voting_right", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("start_date", sa.Date),
        sa.Column("end_date", sa.Date),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_memberships_person_id", "memberships", ["person_id"])
    op.create_index("ix_memberships_organization_id", "memberships", ["organization_id"])

    # Legislative Terms
    op.create_table(
        "legislative_terms",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("start_date", sa.Date),
        sa.Column("end_date", sa.Date),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("body_id", "external_id", name="uq_term_body_external"),
    )

    # Meetings
    op.create_table(
        "meetings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.String(1000)),
        sa.Column("start", sa.DateTime(timezone=True)),
        sa.Column("end", sa.DateTime(timezone=True)),
        sa.Column("location_name", sa.String(500)),
        sa.Column("location_address", sa.Text),
        sa.Column("cancelled", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("meeting_state", sa.String(50)),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("body_id", "external_id", name="uq_meeting_body_external"),
    )
    op.create_index("ix_meetings_body_id", "meetings", ["body_id"])
    op.create_index("ix_meetings_start", "meetings", ["start"])
    op.create_index("ix_meeting_body_start", "meetings", ["body_id", "start"])

    # Agenda Items
    op.create_table(
        "agenda_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "meeting_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("meetings.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("number", sa.String(50)),
        sa.Column("name", sa.Text),
        sa.Column("public", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("result", sa.String(100)),
        sa.Column("resolution_text", sa.Text),
        sa.Column("order", sa.Integer),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_agenda_items_meeting_id", "agenda_items", ["meeting_id"])

    # Papers
    op.create_table(
        "papers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.Text),
        sa.Column("reference", sa.String(255)),
        sa.Column("paper_type", sa.String(100)),
        sa.Column("date", sa.Date),
        sa.Column("summary", sa.Text),
        sa.Column("locations", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("body_id", "external_id", name="uq_paper_body_external"),
    )
    op.create_index("ix_papers_body_id", "papers", ["body_id"])
    op.create_index("ix_papers_reference", "papers", ["reference"])
    op.create_index("ix_papers_date", "papers", ["date"])

    # Consultations
    op.create_table(
        "consultations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "paper_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("papers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "meeting_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("meetings.id", ondelete="SET NULL")
        ),
        sa.Column(
            "agenda_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agenda_items.id", ondelete="SET NULL"),
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("role", sa.String(100)),
        sa.Column("authoritative", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_consultations_paper_id", "consultations", ["paper_id"])
    op.create_index("ix_consultations_meeting_id", "consultations", ["meeting_id"])

    # Files
    op.create_table(
        "files",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("paper_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("papers.id", ondelete="SET NULL")),
        sa.Column(
            "meeting_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("meetings.id", ondelete="SET NULL")
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("name", sa.Text),
        sa.Column("file_name", sa.String(500)),
        sa.Column("mime_type", sa.String(100)),
        sa.Column("size", sa.BigInteger),
        sa.Column("access_url", sa.Text),
        sa.Column("download_url", sa.Text),
        sa.Column("file_date", sa.Date),
        sa.Column("sha256_hash", sa.String(64)),
        sa.Column("text_content", sa.Text),
        sa.Column("text_extraction_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("text_extraction_method", sa.String(50)),
        sa.Column("text_extraction_error", sa.Text),
        sa.Column("text_extracted_at", sa.DateTime(timezone=True)),
        sa.Column("page_count", sa.Integer),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_files_body_id", "files", ["body_id"])
    op.create_index("ix_files_paper_id", "files", ["paper_id"])
    op.create_index("ix_files_sha256_hash", "files", ["sha256_hash"])
    op.create_index("ix_files_text_extraction_status", "files", ["text_extraction_status"])

    # Locations
    op.create_table(
        "locations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "body_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bodies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text, nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("street_address", sa.Text),
        sa.Column("locality", sa.String(200)),
        sa.Column("postal_code", sa.String(20)),
        sa.Column("region", sa.String(100)),
        sa.Column("latitude", sa.Float),
        sa.Column("longitude", sa.Float),
        sa.Column("oparl_created", sa.DateTime(timezone=True)),
        sa.Column("oparl_modified", sa.DateTime(timezone=True)),
        sa.Column("raw", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("deleted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_locations_body_id", "locations", ["body_id"])

    # Sync Logs
    op.create_table(
        "sync_logs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "source_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("sources.id", ondelete="SET NULL")
        ),
        sa.Column("flow_run_id", sa.String(64)),
        sa.Column("sync_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("duration_seconds", sa.Float),
        sa.Column("entities_synced", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("errors", postgresql.JSONB, nullable=False, server_default="[]"),
        sa.Column("triggered_by", sa.String(20), nullable=False, server_default="'schedule'"),
    )
    op.create_index("ix_sync_logs_source_id", "sync_logs", ["source_id"])
    op.create_index("ix_sync_logs_status", "sync_logs", ["status"])

    # Extraction Jobs
    op.create_table(
        "extraction_jobs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "file_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("files.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("method", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="'pending'"),
        sa.Column("error", sa.Text),
        sa.Column("char_count", sa.Integer),
        sa.Column("page_count", sa.Integer),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_extraction_jobs_file_id", "extraction_jobs", ["file_id"])
    op.create_index("ix_extraction_jobs_status", "extraction_jobs", ["status"])


def downgrade() -> None:
    op.drop_table("extraction_jobs")
    op.drop_table("sync_logs")
    op.drop_table("locations")
    op.drop_table("files")
    op.drop_table("consultations")
    op.drop_table("papers")
    op.drop_table("agenda_items")
    op.drop_table("meetings")
    op.drop_table("legislative_terms")
    op.drop_table("memberships")
    op.drop_table("persons")
    op.drop_table("organizations")
    op.drop_table("bodies")
    op.drop_table("sources")
