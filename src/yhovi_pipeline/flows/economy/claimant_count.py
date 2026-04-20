"""Claimant Count flow.

Extracts Universal Credit / JSA claimant count data from DWP Stat-Xplore
for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="economy-claimant-count",
    description="Extract DWP claimant count data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def claimant_count_flow() -> None:
    """Orchestrate the claimant count ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Extract claimant count data from DWP Stat-Xplore API.
        2. Validate response.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    logger = get_run_logger()
    logger.info(
        "DWP Stat-Xplore extract not yet implemented: pending DWP API key "
        "registration at https://stat-xplore.dwp.gov.uk/. Once the key is "
        "available, set DWP_API_KEY and implement tasks/extract/dwp.py."
    )
