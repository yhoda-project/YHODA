"""GDP / GVA flow.

Extracts Gross Value Added (balanced) and regional GDP estimates from the ONS
Regional Accounts publication for Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="economy/gdp-gva",
    description="Extract ONS GVA / regional GDP data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def gdp_gva_flow() -> None:
    """Orchestrate the GVA / GDP ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download ONS Regional Accounts tables for Yorkshire.
        2. Parse XLSX / CSV release.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("gdp_gva_flow not yet implemented")
