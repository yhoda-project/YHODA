"""BEIS / DESNZ sub-national energy extract tasks.

BEIS (now DESNZ) publishes Sub-national Electricity and Gas Consumption
Statistics at LAD level annually.
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/beis/energy-consumption",
    description="Extract BEIS sub-national energy consumption data for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_energy_consumption(reference_year: int) -> pd.DataFrame:
    """Fetch sub-national electricity and gas consumption statistics.

    Args:
        reference_year: The calendar year to extract.

    Returns:
        DataFrame with electricity and gas consumption for Yorkshire LADs.
    """
    # TODO: implement — download from GOV.UK open data portal
    raise NotImplementedError("extract_energy_consumption not yet implemented")
