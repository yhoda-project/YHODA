"""Dataset metadata helpers.

Utility functions for creating and updating ``DatasetMetadata`` records,
used by load tasks to write extraction audit trails.
"""

from __future__ import annotations

from yhovi_pipeline.db.models import DatasetMetadata, ExtractionStatus


def build_metadata_record(
    dataset_code: str,
    source: str,
    status: ExtractionStatus = ExtractionStatus.PENDING,
    prefect_flow_run_id: str | None = None,
) -> DatasetMetadata:
    """Construct a new ``DatasetMetadata`` ORM instance.

    Args:
        dataset_code: Dataset identifier.
        source: Source system identifier.
        status: Initial extraction status.
        prefect_flow_run_id: UUID of the Prefect flow run, if available.

    Returns:
        Unpersisted ``DatasetMetadata`` instance.
    """
    # TODO: implement
    raise NotImplementedError("build_metadata_record not yet implemented")
