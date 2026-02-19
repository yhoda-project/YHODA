"""NOMIS extract tasks.

NOMIS is the ONS labour market statistics API.  It provides access to BRES
(Business Register and Employment Survey) and the Annual Population Survey
(APS), among other datasets.

API docs: https://www.nomisweb.co.uk/api/v01/help
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="extract/nomis/bres",
    description="Extract BRES employment data from NOMIS for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_bres(reference_year: int) -> pd.DataFrame:
    """Fetch Business Register and Employment Survey data from NOMIS.

    Args:
        reference_year: The survey year to extract (e.g. 2023).

    Returns:
        DataFrame with raw NOMIS BRES response for all Yorkshire LADs.
    """
    # TODO: implement
    # settings = get_settings()
    # lad_codes = settings.yorkshire_lad_codes
    # api_key = settings.nomis_api_key
    raise NotImplementedError("extract_bres not yet implemented")


@task(
    name="extract/nomis/aps",
    description="Extract Annual Population Survey data from NOMIS for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_aps(reference_year: int) -> pd.DataFrame:
    """Fetch Annual Population Survey data from NOMIS.

    Args:
        reference_year: The survey year to extract (e.g. 2023).

    Returns:
        DataFrame with raw NOMIS APS response for all Yorkshire LADs.
    """
    # TODO: implement
    raise NotImplementedError("extract_aps not yet implemented")
