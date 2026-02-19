"""Education Attainment flow.

Extracts key stage attainment and qualification data from the DfE / ONS
for all Yorkshire LADs.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="society/education-attainment",
    description="Extract education attainment data for Yorkshire LADs.",
    retries=1,
    retry_delay_seconds=300,
    task_runner=ThreadPoolTaskRunner(max_workers=4),  # type: ignore[arg-type]
)
def education_attainment_flow() -> None:
    """Orchestrate the education attainment ETL pipeline.

    Steps (to be implemented in Phase 2):
        1. Download DfE statistics release for Yorkshire LADs.
        2. Parse and validate the dataset.
        3. Normalise to the canonical ``Indicator`` schema.
        4. Upsert into the data warehouse.
        5. Write audit metadata.
    """
    # TODO: implement — call extract, transform, and load tasks
    raise NotImplementedError("education_attainment_flow not yet implemented")
