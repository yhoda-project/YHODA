"""Air Quality flow.

Extracts air quality monitoring data (PM2.5, PM10, NO2, O3) from the DEFRA
Automatic Urban and Rural Network (AURN) for Yorkshire monitoring stations,
aggregated to LAD level.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="environment/air-quality",
    description="Extract DEFRA AURN air quality data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def air_quality_flow() -> None:
    """Orchestrate the air quality ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Query DEFRA AURN API for monitoring stations in Yorkshire.
        2. Fetch annual mean concentrations.
        3. Spatial join stations to LADs using the geo lookup.
        4. Aggregate to LAD level.
        5. Normalise to the canonical ``Indicator`` schema.
        6. Upsert into the data warehouse.
        7. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("air_quality_flow not yet implemented")
