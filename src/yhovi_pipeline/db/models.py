"""SQLAlchemy 2.0 ORM models for the YHODA data warehouse.

Design notes
------------
* Uses the new annotation-driven ``Mapped[T]`` / ``mapped_column()`` style
  (not the legacy ``Column(...)`` API).
* ``MetaData`` is initialised with a naming convention so Alembic can generate
  stable, deterministic constraint names for SQL Server.
* All ``Enum`` columns use ``native_enum=False`` because SQL Server does not
  support a native ENUM type.
* The ``Indicator`` table has a unique index on ``(indicator_id, lad_code,
  reference_period)`` — this triple is the upsert key used by load tasks.
* ``GeoLookup`` maps LSOA codes → MSOA → LAD → Region, matching the ONS
  December 2021 geography release used throughout the project.
"""

from __future__ import annotations

import enum
from datetime import date, datetime

from sqlalchemy import (
    Date,
    DateTime,
    Enum,
    Index,
    MetaData,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------------------------------------------------------------------
# Naming convention — ensures Alembic generates deterministic constraint names
# on SQL Server (which requires explicit names for FK / CHECK / UQ constraints
# when using autogenerate).
# ---------------------------------------------------------------------------

NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Shared declarative base — all ORM models inherit from this."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ExtractionStatus(enum.StrEnum):
    """Lifecycle state of a dataset extraction run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Indicator(Base):
    """One row per indicator x LAD x reference period observation.

    This is the central fact table.  Each row stores a single statistical
    value (e.g. "claimant rate for Bradford, April 2024").

    The unique index on ``(indicator_id, lad_code, reference_period)``
    supports idempotent upsert operations in the load tasks.
    """

    __tablename__ = "indicator"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    indicator_id: Mapped[str] = mapped_column(String(100), nullable=False)
    """Short machine-readable identifier, e.g. ``"claimant_rate"``."""

    indicator_name: Mapped[str] = mapped_column(String(255), nullable=False)
    """Human-readable display name."""

    lad_code: Mapped[str] = mapped_column(String(9), nullable=False)
    """ONS GSS code for the Local Authority District, e.g. ``"E08000032"``."""

    lad_name: Mapped[str] = mapped_column(String(100), nullable=False)
    """Human-readable LAD name, e.g. ``"Bradford"``."""

    reference_period: Mapped[date] = mapped_column(Date, nullable=False)
    """The date the observation relates to (first day of the period)."""

    value: Mapped[float | None] = mapped_column(nullable=True)
    """The numeric value.  ``NULL`` when suppressed for disclosure control."""

    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    """Unit of measurement, e.g. ``"rate"`` or ``"count"``."""

    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    """Source system identifier, e.g. ``"nomis"`` or ``"fingertips"``."""

    dataset_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    """Dataset / series code within the source system."""

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index(
            "ix_indicator_upsert_key",
            "indicator_id",
            "lad_code",
            "reference_period",
            unique=True,
        ),
    )


class DatasetMetadata(Base):
    """Audit record for each extraction / load run.

    One row is written per flow run, capturing provenance information so
    analysts can trace any observation back to the exact source request.
    """

    __tablename__ = "dataset_metadata"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    dataset_code: Mapped[str] = mapped_column(String(100), nullable=False)
    """Identifier matching ``Indicator.dataset_code``."""

    source: Mapped[str] = mapped_column(String(100), nullable=False)
    """Source system, e.g. ``"nomis"``."""

    extraction_status: Mapped[ExtractionStatus] = mapped_column(
        Enum(ExtractionStatus, native_enum=False, length=20),
        nullable=False,
        default=ExtractionStatus.PENDING,
    )
    """Current lifecycle state of this extraction run."""

    prefect_flow_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    """UUID of the Prefect flow run, for cross-referencing in the Prefect UI."""

    rows_extracted: Mapped[int | None] = mapped_column(nullable=True)
    rows_loaded: Mapped[int | None] = mapped_column(nullable=True)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    """Truncated exception message on failure."""

    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    """API endpoint or file URL that was fetched."""

    extracted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class GeoLookup(Base):
    """ONS geography hierarchy: LSOA → MSOA → LAD → Region.

    Populated once from the ONS Open Geography Portal and used by transform
    tasks to aggregate sub-LAD data up to LAD level.

    Primary key is the LSOA code (unique in the ONS hierarchy).
    """

    __tablename__ = "geo_lookup"

    lsoa_code: Mapped[str] = mapped_column(String(9), primary_key=True)
    """Lower Super Output Area GSS code, e.g. ``"E01000001"``."""

    lsoa_name: Mapped[str] = mapped_column(String(100), nullable=False)

    msoa_code: Mapped[str] = mapped_column(String(9), nullable=False)
    """Middle Super Output Area GSS code, e.g. ``"E02000001"``."""

    msoa_name: Mapped[str] = mapped_column(String(100), nullable=False)

    lad_code: Mapped[str] = mapped_column(String(9), nullable=False)
    """Local Authority District GSS code, e.g. ``"E08000032"``."""

    lad_name: Mapped[str] = mapped_column(String(100), nullable=False)

    region_code: Mapped[str | None] = mapped_column(String(9), nullable=True)
    """ONS region GSS code, e.g. ``"E12000003"`` (Yorkshire & The Humber)."""

    region_name: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (
        Index("ix_geo_lookup_lad_code", "lad_code"),
        Index("ix_geo_lookup_msoa_code", "msoa_code"),
    )
