"""Crime Statistics flow.

Extracts recorded crime data from the Home Office / ONS for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/crime-statistics",
    description="Extract Home Office recorded crime statistics for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def crime_statistics_flow() -> None:
    """Orchestrate the crime statistics ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download ONS / Home Office crime tables for Yorkshire.
        2. Parse and validate the data.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("crime_statistics_flow not yet implemented")
