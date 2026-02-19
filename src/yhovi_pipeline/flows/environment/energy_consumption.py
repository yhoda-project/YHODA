"""Energy Consumption flow.

Extracts sub-national electricity and gas consumption data from BEIS / DESNZ
for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="environment/energy-consumption",
    description="Extract BEIS sub-national energy consumption data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def energy_consumption_flow() -> None:
    """Orchestrate the energy consumption ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download BEIS Sub-national Electricity / Gas Consumption Statistics.
        2. Filter to Yorkshire LADs.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("energy_consumption_flow not yet implemented")
