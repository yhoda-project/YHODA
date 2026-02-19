"""Deprivation / IMD flow.

Extracts Indices of Multiple Deprivation (IMD) scores and ranks from MHCLG
for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/deprivation-imd",
    description="Extract MHCLG Indices of Multiple Deprivation for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def deprivation_imd_flow() -> None:
    """Orchestrate the IMD ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download MHCLG IMD release for Yorkshire LADs.
        2. Parse the LSOA-level scores.
        3. Aggregate to LAD using the geo lookup.
        4. Normalise to the canonical ``Indicator`` schema.
        5. Upsert into the data warehouse.
        6. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("deprivation_imd_flow not yet implemented")
