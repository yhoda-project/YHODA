"""NHS Fingertips extract tasks.

NHS Fingertips (fingertips.phe.org.uk) provides the Public Health Profiles API
with hundreds of public health indicators at LAD and sub-LAD geographies.

API docs: https://fingertips.phe.org.uk/api
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/fingertips/indicators",
    description="Extract health outcome indicators from NHS Fingertips for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_fingertips_indicators(
    profile_id: int,
    indicator_ids: list[int],
) -> pd.DataFrame:
    """Fetch indicator data from the Fingertips API.

    Args:
        profile_id: The Fingertips profile identifier (e.g. 19 for Public Health Outcomes Framework).
        indicator_ids: List of Fingertips indicator IDs to fetch.

    Returns:
        DataFrame with indicator values for Yorkshire LADs.
    """
    # TODO: implement using fingertips-py or direct HTTP requests
    raise NotImplementedError("extract_fingertips_indicators not yet implemented")
