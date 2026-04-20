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

from yhovi_pipeline.config import YORKSHIRE_LAD_CODES

_logger = logging.getLogger(__name__)


def _get_logger():
    try:
        return get_run_logger()
    except Exception:
        return _logger


def _parse_nomis_date(date_name: str) -> date:
    """Parse a Nomis DATE_NAME string to a date.

    Handles two formats:
    - Rolling periods: "Jan 2004-Dec 2004" — returns first day of end month.
    - Plain years: "2023" — returns 1 Jan of that year (ASHE annual data).

    Args:
        date_name: Nomis DATE_NAME value.

    Returns:
        A ``date`` representing the reference period.
    """
    # Rolling period: "Jan 2004-Dec 2004"
    match = re.search(r"-\s*(\w{3})\s+(\d{4})", date_name)
    if match:
        month_str, year_str = match.group(1), match.group(2)
        return datetime.strptime(f"01 {month_str} {year_str}", "%d %b %Y").date()

    # Plain year: "2023"
    year_match = re.fullmatch(r"\d{4}", date_name.strip())
    if year_match:
        return date(int(date_name.strip()), 1, 1)

    raise ValueError(f"Cannot parse Nomis date: {date_name!r}")


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


@task(
    name="transform/normalise/nomis-ashe",
    description="Normalise a Nomis ASHE API response to the canonical Indicator schema.",
)
def normalise_nomis_ashe(
    df: pd.DataFrame,
    dataset_code: str = "eejpay",
) -> pd.DataFrame:
    """Transform a Nomis ASHE API response into the Indicator schema.

    ASHE returns annual data with DATE_NAME as a plain year string (e.g. "2023").

    Args:
        df: Raw DataFrame from ``extract_ashe``.
        dataset_code: Dataset code (default "eejpay").

    Returns:
        DataFrame with columns matching the ``Indicator`` ORM model.
    """
    logger = _get_logger()

    now = datetime.utcnow()
    result = pd.DataFrame(
        {
            "indicator_id": "median_weekly_earnings",
            "indicator_name": "Median gross weekly earnings",
            "lad_code": df["GEOGRAPHY_CODE"].values,
            "lad_name": df["GEOGRAPHY_NAME"].values,
            "reference_period": df["DATE_NAME"].apply(_parse_nomis_date),
            "value": pd.to_numeric(df["OBS_VALUE"], errors="coerce"),
            "unit": "£",
            "source": "nomis",
            "dataset_code": dataset_code,
            "created_at": now,
            "updated_at": now,
        }
    )

    result = result.dropna(subset=["value"])
    logger.info("Normalised %d rows for ASHE median weekly earnings", len(result))
    return result


# ---------------------------------------------------------------------------
# Fingertips helpers
# ---------------------------------------------------------------------------


def _parse_fingertips_period(period: str) -> date:
    """Parse a Fingertips time period string to a ``date``.

    Handles three common formats returned by the Fingertips API:

    - Rolling average ``"2018 - 20"`` → ``date(2020, 1, 1)`` (end year)
    - Financial year ``"2019/20"``     → ``date(2020, 1, 1)`` (end year)
    - Single year    ``"2021"``        → ``date(2021, 1, 1)``

    Args:
        period: Raw ``Time period`` string from the Fingertips CSV.

    Returns:
        A ``date`` representing the end of the reported period.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    period = period.strip()

    # "2018 - 20" or "2018-20" — rolling average, take end year
    m = re.fullmatch(r"(\d{4})\s*-\s*(\d{2})", period)
    if m:
        century = m.group(1)[:2]
        return date(int(century + m.group(2)), 1, 1)

    # "2019/20" — financial year, take end year
    m = re.fullmatch(r"(\d{4})/(\d{2})", period)
    if m:
        century = m.group(1)[:2]
        return date(int(century + m.group(2)), 1, 1)

    # "2021" — single calendar year
    if re.fullmatch(r"\d{4}", period):
        return date(int(period), 1, 1)

    raise ValueError(f"Cannot parse Fingertips time period: {period!r}")


@task(
    name="transform/normalise/fingertips",
    description="Normalise a Fingertips API response to the canonical Indicator schema.",
)
def normalise_fingertips(
    df: pd.DataFrame,
    dataset_code: str,
    indicator_id: str,
    indicator_name: str,
    sex_filter: str,
    unit: str | None = None,
) -> pd.DataFrame:
    """Transform a Fingertips API response into the canonical Indicator schema.

    Filters the raw England-wide DataFrame down to Yorkshire LADs and the
    requested sex dimension, then maps columns to the ``Indicator`` ORM shape.

    Args:
        df: Raw DataFrame from ``extract_fingertips_indicators``.
        dataset_code: Internal dataset code (e.g. ``"sheleb_m"``).
        indicator_id: Machine-readable indicator identifier stored in the DB
            (e.g. ``"life_expectancy_male"``).
        indicator_name: Human-readable indicator name.
        sex_filter: Value of the ``Sex`` column to keep
            (``"Male"``, ``"Female"``, or ``"Persons"``).
        unit: Optional unit of measurement (e.g. ``"Years"``).

    Returns:
        DataFrame with columns matching the ``Indicator`` ORM model.
    """
    logger = _get_logger()

    # Filter to the requested sex dimension
    df = df[df["Sex"] == sex_filter].copy()

    # Filter to Yorkshire LADs
    df = df[df["Area Code"].isin(YORKSHIRE_LAD_CODES)].copy()

    if df.empty:
        raise ValueError(
            f"No Fingertips data for sex={sex_filter!r} in Yorkshire LADs "
            f"(dataset_code={dataset_code!r}). Check indicator ID and sex filter."
        )

    # Drop rows with no time period, then cast to str before parsing —
    # Fingertips returns mixed-type columns (some years arrive as integers).
    df = df.dropna(subset=["Time period"]).copy()
    df["reference_period"] = df["Time period"].astype(str).apply(_parse_fingertips_period)

    now = datetime.utcnow()
    result = pd.DataFrame(
        {
            "indicator_id": indicator_id,
            "indicator_name": indicator_name,
            "lad_code": df["Area Code"].values,
            "lad_name": df["Area Name"].values,
            "reference_period": df["reference_period"].values,
            "value": pd.to_numeric(df["Value"], errors="coerce"),
            "unit": unit,
            "source": "fingertips",
            "dataset_code": dataset_code,
            "created_at": now,
            "updated_at": now,
        }
    )

    result = result.dropna(subset=["value"])

    # Fingertips returns multiple age groups per LAD/period/sex for some indicators.
    # Deduplicate on the upsert key, keeping the last occurrence.
    before = len(result)
    result = result.drop_duplicates(
        subset=["indicator_id", "lad_code", "reference_period"], keep="last"
    )
    if len(result) < before:
        logger.warning(
            "Dropped %d duplicate rows for %s (%s) — multiple age groups in source",
            before - len(result),
            indicator_id,
            sex_filter,
        )

    logger.info(
        "Normalised %d rows for %s (%s) from Fingertips",
        len(result),
        indicator_id,
        sex_filter,
    )
    return result
