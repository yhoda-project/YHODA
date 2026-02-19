"""ONS extract tasks.

Extracts data from ONS open data APIs and bulk download endpoints,
including Regional Accounts (GVA), Business Demography, and Census tables.
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/ons/regional-accounts",
    description="Download ONS Regional Accounts (GVA) data for Yorkshire.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_regional_accounts(reference_year: int) -> pd.DataFrame:
    """Fetch ONS Regional Accounts publication data.

    Args:
        reference_year: The reference year to extract.

    Returns:
        DataFrame with raw GVA / GDP data for Yorkshire LADs.
    """
    # TODO: implement
    raise NotImplementedError("extract_regional_accounts not yet implemented")


@task(
    name="extract/ons/business-demography",
    description="Download ONS Business Demography release for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_business_demography(reference_year: int) -> pd.DataFrame:
    """Fetch ONS Business Demography publication data.

    Args:
        reference_year: The reference year to extract.

    Returns:
        DataFrame with business births, deaths, and survival data.
    """
    # TODO: implement
    raise NotImplementedError("extract_business_demography not yet implemented")


@task(
    name="extract/ons/housing-tenure",
    description="Download ONS housing tenure data for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_housing_tenure(reference_year: int) -> pd.DataFrame:
    """Fetch ONS housing tenure statistics.

    Args:
        reference_year: The reference year to extract.

    Returns:
        DataFrame with housing tenure breakdowns for Yorkshire LADs.
    """
    # TODO: implement
    raise NotImplementedError("extract_housing_tenure not yet implemented")
