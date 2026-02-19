"""SQL Server load tasks.

Writes normalised ``Indicator`` rows to the SQL Server data warehouse using
an idempotent upsert strategy (MERGE or DELETE + INSERT).
"""

from __future__ import annotations

import pandas as pd
from prefect import task

from yhovi_pipeline.db.models import ExtractionStatus


@task(
    name="load/sql-server/upsert-indicators",
    description="Upsert a normalised Indicator DataFrame into the SQL Server data warehouse.",
)
def upsert_indicators(df: pd.DataFrame, dataset_code: str) -> int:
    """Upsert rows into the ``indicator`` table.

    Uses the unique index on ``(indicator_id, lad_code, reference_period)``
    as the merge key.  Existing rows are updated; new rows are inserted.

    Args:
        df: Normalised DataFrame matching the ``Indicator`` schema.
        dataset_code: Dataset identifier for logging.

    Returns:
        Number of rows upserted.
    """
    # TODO: implement
    # settings = get_settings()
    # engine = create_engine(settings.sql_server_connection_string.get_secret_value())
    raise NotImplementedError("upsert_indicators not yet implemented")


@task(
    name="load/sql-server/write-metadata",
    description="Write a DatasetMetadata audit record to the data warehouse.",
)
def write_metadata(
    dataset_code: str,
    source: str,
    status: ExtractionStatus,
    prefect_flow_run_id: str | None = None,
    rows_extracted: int | None = None,
    rows_loaded: int | None = None,
    error_message: str | None = None,
    source_url: str | None = None,
) -> None:
    """Insert an audit record into the ``dataset_metadata`` table.

    Args:
        dataset_code: Dataset identifier.
        source: Source system identifier.
        status: Final ``ExtractionStatus`` for this run.
        prefect_flow_run_id: UUID of the Prefect flow run, if available.
        rows_extracted: Number of rows returned by the extract step.
        rows_loaded: Number of rows written to the warehouse.
        error_message: Truncated exception message on failure.
        source_url: API endpoint or file URL that was fetched.
    """
    # TODO: implement
    raise NotImplementedError("write_metadata not yet implemented")
