"""Employment & Jobs flow.

Extracts employment count and rate data from NOMIS (BRES / APS datasets)
for all Yorkshire LADs and loads them into the data warehouse.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="economy/employment-jobs",
    description="Extract employment and jobs data from NOMIS for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def employment_jobs_flow() -> None:
    """Orchestrate the employment & jobs ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Extract BRES / APS data from NOMIS API for all Yorkshire LADs.
        2. Validate raw API response schema.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert rows into the SQL Server data warehouse.
        5. Write ``DatasetMetadata`` audit record.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("employment_jobs_flow not yet implemented")
