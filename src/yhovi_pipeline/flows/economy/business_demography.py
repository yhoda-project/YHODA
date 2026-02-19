"""Business Demography flow.

Extracts business births, deaths, and survival rates from the ONS Business
Demography publication for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="economy/business-demography",
    description="Extract ONS business demography data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def business_demography_flow() -> None:
    """Orchestrate the business demography ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download ONS Business Demography release for Yorkshire LADs.
        2. Parse and validate the dataset.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("business_demography_flow not yet implemented")
