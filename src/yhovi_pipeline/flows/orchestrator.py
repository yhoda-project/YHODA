"""Master orchestrator flow.

Runs all domain flows in dependency order.  Useful for a full refresh
or for triggering from an external event.
"""

from __future__ import annotations

from prefect import flow
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

from yhovi_pipeline.flows.economy.business_demography import business_demography_flow
from yhovi_pipeline.flows.economy.claimant_count import claimant_count_flow
from yhovi_pipeline.flows.economy.earnings import earnings_flow
from yhovi_pipeline.flows.economy.employment_jobs import employment_jobs_flow
from yhovi_pipeline.flows.economy.gdp_gva import gdp_gva_flow
from yhovi_pipeline.flows.environment.air_quality import air_quality_flow
from yhovi_pipeline.flows.environment.energy_consumption import energy_consumption_flow
from yhovi_pipeline.flows.society.crime_statistics import crime_statistics_flow
from yhovi_pipeline.flows.society.deprivation_imd import deprivation_imd_flow
from yhovi_pipeline.flows.society.digital_inclusion import digital_inclusion_flow
from yhovi_pipeline.flows.society.education_attainment import education_attainment_flow
from yhovi_pipeline.flows.society.health_outcomes import health_outcomes_flow
from yhovi_pipeline.flows.society.housing_tenure import housing_tenure_flow
from yhovi_pipeline.flows.society.physical_activity import physical_activity_flow


@flow(
    name="orchestrator/full-refresh",
    description="Run all YHODA domain flows in sequence.",
    retries=0,
    task_runner=ThreadPoolTaskRunner(max_workers=1),  # type: ignore[arg-type]
)
def full_refresh_flow() -> None:
    """Trigger all economy, society, and environment flows in sequence.

    Each sub-flow is called directly (Prefect v3 flow-of-flows pattern).
    Flows that have no automated source log an informational message and
    return immediately — they do not raise errors.
    """
    logger = get_run_logger()

    logger.info("=== Starting full refresh ===")

    # ── Economy ───────────────────────────────────────────────────────────────
    logger.info("--- Economy flows ---")
    employment_jobs_flow()
    earnings_flow()
    claimant_count_flow()
    business_demography_flow()
    gdp_gva_flow()

    # ── Society ───────────────────────────────────────────────────────────────
    logger.info("--- Society flows ---")
    health_outcomes_flow()
    education_attainment_flow()
    housing_tenure_flow()
    deprivation_imd_flow()
    crime_statistics_flow()
    physical_activity_flow()
    digital_inclusion_flow()

    # ── Environment ───────────────────────────────────────────────────────────
    logger.info("--- Environment flows ---")
    air_quality_flow()
    energy_consumption_flow()

    logger.info("=== Full refresh complete ===")
