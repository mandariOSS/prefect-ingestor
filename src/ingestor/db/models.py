"""
SQLAlchemy 2.0 Models.

Datenmodell: Jede OParl-Entität erhält eine eigene Tabelle mit
einem relationalen Kern (IDs, Fremdschlüssel, Timestamps) und einem
JSONB-Feld `raw`, das das vollständige OParl-JSON abspeichert.

Vorteile:
- Relationale Queries schnell (Index auf body_id, external_id, modified_at)
- Flexibilität bei OParl-Feldänderungen (kein Migrationszwang)
- Exakte OParl-API-Rekonstruktion aus raw möglich

Audit-Tabellen: sources, sync_logs, extraction_jobs.
"""

from __future__ import annotations

from datetime import datetime, date
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """SQLAlchemy Declarative Base."""


# =============================================================================
# Mixins
# =============================================================================


class TimestampMixin:
    """Mixin für created_at/updated_at."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class OParlEntityMixin:
    """
    Gemeinsame Felder aller OParl-Entitäten.

    - external_id: OParl-URL, die als ID dient (eindeutig pro Quelle)
    - oparl_created/modified: Zeitstempel aus OParl-Daten
    - raw: Vollständiges OParl-JSON
    - deleted: OParl-1.1 soft-delete-Flag
    """

    external_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    oparl_created: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    oparl_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)


# =============================================================================
# Management: Sources, Sync-Logs, Extraction-Jobs
# =============================================================================


class Source(Base, TimestampMixin):
    """Eine verwaltete OParl-Quelle (Kommune)."""

    __tablename__ = "sources"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    system_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sync_interval_min: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_full_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    bodies: Mapped[list[Body]] = relationship(back_populates="source", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Source {self.name}>"


class SyncLog(Base):
    """Protokoll pro Sync-Zyklus."""

    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("sources.id", ondelete="SET NULL"), index=True
    )
    flow_run_id: Mapped[str | None] = mapped_column(String(64), index=True)
    sync_type: Mapped[str] = mapped_column(String(20), nullable=False)  # incremental | full
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # success | failed | partial | running
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column()
    entities_synced: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    errors: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    triggered_by: Mapped[str] = mapped_column(String(20), nullable=False, default="schedule")


class ExtractionJob(Base):
    """Job für PDF-Text-Extraktion."""

    __tablename__ = "extraction_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    file_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("files.id", ondelete="CASCADE"), nullable=False, index=True
    )
    method: Mapped[str] = mapped_column(String(20), nullable=False)  # pypdf | tesseract | mistral
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    error: Mapped[str | None] = mapped_column(Text)
    char_count: Mapped[int | None] = mapped_column(Integer)
    page_count: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# =============================================================================
# OParl Entities
# =============================================================================


class Body(Base, TimestampMixin, OParlEntityMixin):
    """Körperschaft (Stadt, Kreis, Gemeinde)."""

    __tablename__ = "bodies"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("sources.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    short_name: Mapped[str | None] = mapped_column(String(100))
    website: Mapped[str | None] = mapped_column(Text)

    source: Mapped[Source] = relationship(back_populates="bodies")

    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_body_source_external"),
    )


class Organization(Base, TimestampMixin, OParlEntityMixin):
    """Gremium/Fraktion/Partei."""

    __tablename__ = "organizations"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    short_name: Mapped[str | None] = mapped_column(String(100))
    organization_type: Mapped[str | None] = mapped_column(String(100))
    classification: Mapped[str | None] = mapped_column(String(100))
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)

    __table_args__ = (
        UniqueConstraint("body_id", "external_id", name="uq_org_body_external"),
    )


class Person(Base, TimestampMixin, OParlEntityMixin):
    """Ratsmitglied/Sachkundige Person."""

    __tablename__ = "persons"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    family_name: Mapped[str | None] = mapped_column(String(200))
    given_name: Mapped[str | None] = mapped_column(String(200))
    title: Mapped[str | None] = mapped_column(String(100))
    email: Mapped[str | None] = mapped_column(String(255))
    phone: Mapped[str | None] = mapped_column(String(100))

    # Mandari-Erweiterung: Personenbild (optional heruntergeladen)
    photo_url: Mapped[str | None] = mapped_column(Text)
    photo_downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    photo_mime_type: Mapped[str | None] = mapped_column(String(100))
    photo_data: Mapped[bytes | None] = mapped_column()  # BYTEA

    __table_args__ = (
        UniqueConstraint("body_id", "external_id", name="uq_person_body_external"),
    )


class Membership(Base, TimestampMixin, OParlEntityMixin):
    """Mitgliedschaft einer Person in einer Organisation."""

    __tablename__ = "memberships"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    person_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("persons.id", ondelete="CASCADE"), nullable=False, index=True
    )
    organization_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    role: Mapped[str | None] = mapped_column(String(255))
    voting_right: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)

    __table_args__ = (Index("ix_membership_active", "end_date"),)


class LegislativeTerm(Base, TimestampMixin, OParlEntityMixin):
    """Wahlperiode."""

    __tablename__ = "legislative_terms"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)

    __table_args__ = (
        UniqueConstraint("body_id", "external_id", name="uq_term_body_external"),
    )


class Meeting(Base, TimestampMixin, OParlEntityMixin):
    """Sitzung."""

    __tablename__ = "meetings"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str | None] = mapped_column(String(1000))
    start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    location_name: Mapped[str | None] = mapped_column(String(500))
    location_address: Mapped[str | None] = mapped_column(Text)
    cancelled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    meeting_state: Mapped[str | None] = mapped_column(String(50))

    __table_args__ = (
        UniqueConstraint("body_id", "external_id", name="uq_meeting_body_external"),
        Index("ix_meeting_body_start", "body_id", "start"),
    )


class AgendaItem(Base, TimestampMixin, OParlEntityMixin):
    """Tagesordnungspunkt."""

    __tablename__ = "agenda_items"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    meeting_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("meetings.id", ondelete="CASCADE"), nullable=False, index=True
    )
    number: Mapped[str | None] = mapped_column(String(50))
    name: Mapped[str | None] = mapped_column(Text)
    public: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    result: Mapped[str | None] = mapped_column(String(100))
    resolution_text: Mapped[str | None] = mapped_column(Text)
    order: Mapped[int | None] = mapped_column(Integer)


class Paper(Base, TimestampMixin, OParlEntityMixin):
    """Vorlage/Drucksache."""

    __tablename__ = "papers"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str | None] = mapped_column(Text)
    reference: Mapped[str | None] = mapped_column(String(255), index=True)
    paper_type: Mapped[str | None] = mapped_column(String(100))
    date: Mapped[date | None] = mapped_column(Date, index=True)

    # Mandari-Erweiterungen
    summary: Mapped[str | None] = mapped_column(Text)  # KI-generiert
    locations: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    __table_args__ = (
        UniqueConstraint("body_id", "external_id", name="uq_paper_body_external"),
    )


class Consultation(Base, TimestampMixin, OParlEntityMixin):
    """Beratung: Paper in AgendaItem in Meeting."""

    __tablename__ = "consultations"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    paper_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("papers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    meeting_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("meetings.id", ondelete="SET NULL"), index=True
    )
    agenda_item_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("agenda_items.id", ondelete="SET NULL"), index=True
    )
    role: Mapped[str | None] = mapped_column(String(100))
    authoritative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class File(Base, TimestampMixin, OParlEntityMixin):
    """Datei (z.B. PDF-Anhang zu Paper)."""

    __tablename__ = "files"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    paper_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("papers.id", ondelete="SET NULL"), index=True
    )
    meeting_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("meetings.id", ondelete="SET NULL"), index=True
    )
    name: Mapped[str | None] = mapped_column(Text)
    file_name: Mapped[str | None] = mapped_column(String(500))
    mime_type: Mapped[str | None] = mapped_column(String(100))
    size: Mapped[int | None] = mapped_column(BigInteger)
    access_url: Mapped[str | None] = mapped_column(Text)
    download_url: Mapped[str | None] = mapped_column(Text)
    file_date: Mapped[date | None] = mapped_column(Date)
    sha256_hash: Mapped[str | None] = mapped_column(String(64), index=True)

    # Mandari-Erweiterungen: Text-Extraktion
    text_content: Mapped[str | None] = mapped_column(Text)
    text_extraction_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    text_extraction_method: Mapped[str | None] = mapped_column(String(50))
    text_extraction_error: Mapped[str | None] = mapped_column(Text)
    text_extracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    page_count: Mapped[int | None] = mapped_column(Integer)


class Location(Base, TimestampMixin, OParlEntityMixin):
    """Ort/Adresse."""

    __tablename__ = "locations"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    body_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("bodies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    description: Mapped[str | None] = mapped_column(Text)
    street_address: Mapped[str | None] = mapped_column(Text)
    locality: Mapped[str | None] = mapped_column(String(200))
    postal_code: Mapped[str | None] = mapped_column(String(20))
    region: Mapped[str | None] = mapped_column(String(100))
    latitude: Mapped[float | None] = mapped_column()
    longitude: Mapped[float | None] = mapped_column()
