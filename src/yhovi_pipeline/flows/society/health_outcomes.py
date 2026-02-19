"""Health Outcomes flow.

Extracts health outcome indicators from NHS Fingertips (Public Health Profiles)
for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/health-outcomes",
    description="Extract NHS Fingertips health outcome indicators for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def health_outcomes_flow() -> None:
    """Orchestrate the health outcomes ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Extract indicator data from the Fingertips API.
        2. Filter to Yorkshire LADs and relevant profiles.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("health_outcomes_flow not yet implemented")
