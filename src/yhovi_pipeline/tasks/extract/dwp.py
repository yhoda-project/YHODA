"""DWP Stat-Xplore extract tasks.

DWP Stat-Xplore provides access to benefit claimant statistics including
Universal Credit and legacy Jobseeker's Allowance (JSA) claimant counts.

API docs: https://stat-xplore.dwp.gov.uk/webapi/online-help/Open-Data-API.html
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/dwp/claimant-count",
    description="Extract Universal Credit / JSA claimant count from DWP Stat-Xplore.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_claimant_count(reference_month: str) -> pd.DataFrame:
    """Fetch claimant count data from DWP Stat-Xplore.

    Args:
        reference_month: ISO 8601 year-month string, e.g. ``"2024-04"``.

    Returns:
        DataFrame with claimant count by LAD for the given month.
    """
    # TODO: implement
    # settings = get_settings()
    # api_key = settings.dwp_api_key.get_secret_value()
    raise NotImplementedError("extract_claimant_count not yet implemented")
