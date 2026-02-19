"""DEFRA AURN extract tasks.

The Automatic Urban and Rural Network (AURN) is managed by DEFRA and provides
hourly air quality monitoring data from sites across England.
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/defra/aurn",
    description="Extract DEFRA AURN air quality data for Yorkshire monitoring stations.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_aurn(reference_year: int) -> pd.DataFrame:
    """Fetch annual mean air quality data from DEFRA AURN.

    Args:
        reference_year: The calendar year to extract.

    Returns:
        DataFrame with annual mean concentrations (PM2.5, PM10, NO2, O3)
        for Yorkshire AURN monitoring stations.
    """
    # TODO: implement — use openair R API or direct DEFRA data download
    raise NotImplementedError("extract_aurn not yet implemented")
