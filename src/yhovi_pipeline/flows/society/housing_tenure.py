"""Housing Tenure flow.

Extracts housing tenure statistics (owner-occupied, rented, social housing)
from ONS Census / MHCLG data for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/housing-tenure",
    description="Extract housing tenure statistics for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def housing_tenure_flow() -> None:
    """Orchestrate the housing tenure ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Extract tenure data from ONS / MHCLG sources.
        2. Validate and normalise.
        3. Upsert into the data warehouse.
        4. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("housing_tenure_flow not yet implemented")
