"""NOMIS extract tasks.

NOMIS is the ONS labour market statistics API.  It provides access to BRES
(Business Register and Employment Survey) and the Annual Population Survey
(APS), among other datasets.

API docs: https://www.nomisweb.co.uk/api/v01/help
"""

from __future__ import annotations

from io import StringIO

import pandas as pd
import requests
from prefect import task
from prefect.logging import get_run_logger

from yhovi_pipeline.config import get_settings

BASE_URL = "https://www.nomisweb.co.uk/api/v01/dataset"

# APS percentage dataset (NM_17_5) variable codes for aged 16-64.
APS_VARIABLES: dict[str, int] = {
    "employment_rate": 45,
    "unemployment_rate": 84,
    "self_employment_rate": 74,
    "econ_inactive_rate": 111,
}

# Columns to request from the API.
_APS_SELECT = (
    "date_name,geography_name,geography_code,variable_name,variable_code,obs_value"
)


def _build_nomis_url(
    dataset: str,
    *,
    geography: list[str],
    variable: list[int],
    measures: int = 20599,
    time: str = "latest",
    select: str | None = None,
    uid: str | None = None,
) -> str:
    """Build a Nomis API CSV request URL.

    Args:
        dataset: Nomis dataset ID, e.g. "NM_17_5".
        geography: List of ONS GSS codes.
        variable: List of variable codes.
        measures: Measures code (20599 = percentage value).
        time: Time parameter, e.g. "latest" or "2023-12".
        select: Comma-separated column names to return.
        uid: Optional Nomis API key (uid) for higher rate limits.
    """
    params = {
        "geography": ",".join(geography),
        "variable": ",".join(str(v) for v in variable),
        "measures": str(measures),
        "time": time,
    }
    if select:
        params["select"] = select
    if uid:
        params["uid"] = uid

    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}/{dataset}.data.csv?{query}"


def _fetch_nomis_csv(url: str) -> pd.DataFrame:
    """Fetch a CSV from the Nomis API and return as a DataFrame."""
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


@task(
    name="extract/nomis/aps",
    description="Extract Annual Population Survey data from NOMIS for Yorkshire LADs.",
    retries=3,
    retry_delay_seconds=60,
)
def extract_aps(
    variable: str,
    time: str = "latest",
) -> pd.DataFrame:
    """Fetch Annual Population Survey percentage data from NOMIS.

    Args:
        variable: Key into APS_VARIABLES, e.g. "employment_rate".
        time: Time parameter — "latest", a specific period like "2023-12",
            or a range like "2004-12-2024-12".

    Returns:
        DataFrame with columns: date_name, geography_name, geography_code,
        variable_name, variable_code, obs_value.
    """
    logger = get_run_logger()
    settings = get_settings()

    if variable not in APS_VARIABLES:
        raise ValueError(
            f"Unknown APS variable {variable!r}. "
            f"Valid keys: {list(APS_VARIABLES.keys())}"
        )

    lad_codes = settings.yorkshire_lad_codes
    uid = (
        settings.nomis_api_key.get_secret_value()
        if settings.nomis_api_key
        else None
    )

    url = _build_nomis_url(
        "NM_17_5",
        geography=lad_codes,
        variable=[APS_VARIABLES[variable]],
        time=time,
        select=_APS_SELECT,
        uid=uid,
    )

    logger.info("Fetching APS %s from Nomis: %s", variable, url if not uid else url.split("uid=")[0] + "uid=***")
    df = _fetch_nomis_csv(url)
    logger.info("Received %d rows for APS %s", len(df), variable)

    return df


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
    raise NotImplementedError("extract_bres not yet implemented")
