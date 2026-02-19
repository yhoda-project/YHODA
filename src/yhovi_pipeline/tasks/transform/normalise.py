"""Normalisation transform tasks.

Converts source-specific DataFrames into the canonical ``Indicator`` schema
used by the load tasks.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from prefect import task


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
    # TODO: implement — rename columns, set constants, handle nulls
    raise NotImplementedError("normalise_to_indicator not yet implemented")
