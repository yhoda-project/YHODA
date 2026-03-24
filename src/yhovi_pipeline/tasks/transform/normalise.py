"""Normalisation transform tasks.

Converts source-specific DataFrames into the canonical ``Indicator`` schema
used by the load tasks.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

_logger = logging.getLogger(__name__)


def _get_logger():
    try:
        return get_run_logger()
    except Exception:
        return _logger


def _parse_nomis_date(date_name: str) -> date:
    """Parse a Nomis DATE_NAME string to a date.

    Nomis returns rolling periods like "Jan 2004-Dec 2004" or
    "Oct 2024-Sep 2025". We take the end date of the period.

    Args:
        date_name: Nomis DATE_NAME value, e.g. "Jan 2004-Dec 2004".

    Returns:
        First day of the end month, e.g. date(2004, 12, 1).
    """
    # Extract the end portion after the hyphen
    match = re.search(r"-\s*(\w{3})\s+(\d{4})", date_name)
    if not match:
        raise ValueError(f"Cannot parse Nomis date: {date_name!r}")

    month_str, year_str = match.group(1), match.group(2)
    return datetime.strptime(f"01 {month_str} {year_str}", "%d %b %Y").date()


@task(
    name="transform/normalise/to-indicator",
    description="Normalise a source DataFrame to the canonical Indicator schema.",
)
def normalise_to_indicator(
    df: pd.DataFrame,
    indicator_id: str,
    indicator_name: str,
    source: str,
    dataset_code: str,
    reference_period: date,
    lad_col: str,
    lad_name_col: str,
    value_col: str,
    unit: str | None = None,
) -> pd.DataFrame:
    """Map source-specific columns to the canonical ``Indicator`` schema.

    Args:
        df: Input DataFrame from the validate step.
        indicator_id: Machine-readable indicator identifier.
        indicator_name: Human-readable indicator name.
        source: Source system identifier (e.g. ``"nomis"``).
        dataset_code: Dataset / series code within the source.
        reference_period: The date the observations relate to.
        lad_col: Column name for LAD GSS code.
        lad_name_col: Column name for LAD name.
        value_col: Column name for the numeric value.
        unit: Optional unit of measurement.

    Returns:
        DataFrame with columns matching the ``Indicator`` ORM model.
    """
    logger = _get_logger()

    now = datetime.utcnow()
    result = pd.DataFrame(
        {
            "indicator_id": indicator_id,
            "indicator_name": indicator_name,
            "lad_code": df[lad_col].values,
            "lad_name": df[lad_name_col].values,
            "reference_period": reference_period,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "unit": unit,
            "source": source,
            "dataset_code": dataset_code,
            "created_at": now,
            "updated_at": now,
        }
    )

    result = result.dropna(subset=["value"])
    logger.info("Normalised %d rows for %s", len(result), indicator_id)
    return result


@task(
    name="transform/normalise/nomis-aps",
    description="Normalise a Nomis APS API response to the canonical Indicator schema.",
)
def normalise_nomis_aps(
    df: pd.DataFrame,
    indicator_id: str,
    indicator_name: str,
    dataset_code: str,
    unit: str = "%",
) -> pd.DataFrame:
    """Transform a Nomis APS API response into the Indicator schema.

    Handles the Nomis-specific column names (uppercase) and date format
    (rolling periods like "Jan 2004-Dec 2004").

    Args:
        df: Raw DataFrame from ``extract_aps``.
        indicator_id: Machine-readable indicator identifier.
        indicator_name: Human-readable indicator name.
        dataset_code: Dataset code, e.g. "eejer".
        unit: Unit of measurement (default "%").

    Returns:
        DataFrame with columns matching the ``Indicator`` ORM model.
    """
    logger = _get_logger()

    now = datetime.utcnow()
    result = pd.DataFrame(
        {
            "indicator_id": indicator_id,
            "indicator_name": indicator_name,
            "lad_code": df["GEOGRAPHY_CODE"].values,
            "lad_name": df["GEOGRAPHY_NAME"].values,
            "reference_period": df["DATE_NAME"].apply(_parse_nomis_date),
            "value": pd.to_numeric(df["OBS_VALUE"], errors="coerce"),
            "unit": unit,
            "source": "nomis",
            "dataset_code": dataset_code,
            "created_at": now,
            "updated_at": now,
        }
    )

    result = result.dropna(subset=["value"])
    logger.info("Normalised %d rows for %s from Nomis APS", len(result), indicator_id)
    return result
