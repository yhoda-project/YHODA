"""Master orchestrator flow.

Runs all domain flows in dependency order.  Useful for a full refresh
or for triggering from an external event.
"""

from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner


@flow(
    name="orchestrator/full-refresh",
    description="Run all YHODA domain flows in sequence.",
    retries=0,
    task_runner=ThreadPoolTaskRunner(max_workers=1),  # type: ignore[arg-type]
)
def full_refresh_flow() -> None:
    """Trigger all economy, society, and environment flows.

    Steps (to be implemented in Phase 2/3):
        1. Run economy flows (employment, claimant count, business demography, GVA).
        2. Run society flows (health, education, housing, deprivation, crime,
           physical activity, digital inclusion).
        3. Run environment flows (air quality, energy consumption).
        4. Report summary statistics.
    """
    # TODO: implement — submit sub-flows as tasks or use Prefect's flow-of-flows
    raise NotImplementedError("full_refresh_flow not yet implemented")
