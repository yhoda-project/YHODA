"""Data validation transform tasks.

Validates raw extracted DataFrames against expected schemas and business rules
before passing them to the normalise step.
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="transform/validate/schema",
    description="Validate a raw extracted DataFrame against the expected schema.",
)
def validate_schema(
    df: pd.DataFrame,
    required_columns: list[str],
    source: str,
) -> pd.DataFrame:
    """Check that a DataFrame has the required columns and non-zero rows.

    Args:
        df: The raw extracted DataFrame to validate.
        required_columns: List of column names that must be present.
        source: Source system name (used in error messages).

    Returns:
        The validated DataFrame (unchanged if validation passes).

    Raises:
        ValueError: If required columns are missing or the DataFrame is empty.
    """
    # TODO: implement
    raise NotImplementedError("validate_schema not yet implemented")


@task(
    name="transform/validate/yorkshire-lads",
    description="Check that all expected Yorkshire LAD codes are present in the data.",
)
def validate_yorkshire_lads(df: pd.DataFrame, lad_col: str = "lad_code") -> pd.DataFrame:
    """Warn (but do not fail) if any expected Yorkshire LAD codes are absent.

    Args:
        df: DataFrame containing a LAD code column.
        lad_col: Name of the LAD code column.

    Returns:
        The input DataFrame unchanged.
    """
    # TODO: implement — compare df[lad_col].unique() against YORKSHIRE_LAD_CODES
    raise NotImplementedError("validate_yorkshire_lads not yet implemented")
