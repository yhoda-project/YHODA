"""Physical Activity flow.

Extracts physical activity participation data from Sport England's Active Lives
survey for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/physical-activity",
    description="Extract Sport England Active Lives physical activity data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def physical_activity_flow() -> None:
    """Orchestrate the physical activity ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Fetch Active Lives data from Sport England API / open data portal.
        2. Filter to Yorkshire LADs.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("physical_activity_flow not yet implemented")
