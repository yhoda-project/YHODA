"""Geo aggregation transform tasks.

Aggregates sub-LAD data (LSOA / MSOA level) up to LAD level using the
``GeoLookup`` table.
"""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(
    name="transform/geo/aggregate-to-lad",
    description="Aggregate sub-LAD data to LAD level using the ONS geo lookup.",
)
def aggregate_to_lad(
    df: pd.DataFrame,
    value_col: str,
    geo_col: str = "lsoa_code",
) -> pd.DataFrame:
    """Join sub-LAD data to the geo lookup and aggregate to LAD level.

    Args:
        df: Input DataFrame with sub-LAD rows.
        value_col: Name of the numeric column to aggregate.
        geo_col: Name of the geography code column in ``df`` (default: ``lsoa_code``).

    Returns:
        DataFrame aggregated to LAD level with columns ``[lad_code, lad_name, value]``.
    """
    # TODO: implement — query GeoLookup, merge, groupby lad_code, aggregate
    raise NotImplementedError("aggregate_to_lad not yet implemented")
