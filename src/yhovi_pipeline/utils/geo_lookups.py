"""Geography lookup utilities.

Helpers for loading and caching the ONS geography hierarchy from the
``GeoLookup`` table, used by transform tasks.
"""

from __future__ import annotations

import functools

import pandas as pd


@functools.lru_cache(maxsize=1)
def get_geo_lookup() -> pd.DataFrame:
    """Load the full LSOA → MSOA → LAD → Region lookup into a DataFrame.

    The result is cached in memory for the duration of the process so that
    multiple tasks in the same flow run do not re-query the database.

    Returns:
        DataFrame with columns: ``lsoa_code``, ``lsoa_name``, ``msoa_code``,
        ``msoa_name``, ``lad_code``, ``lad_name``, ``region_code``, ``region_name``.
    """
    # TODO: implement — query GeoLookup table via SQLAlchemy session
    raise NotImplementedError("get_geo_lookup not yet implemented")


def lsoa_to_lad(lsoa_code: str) -> str | None:
    """Look up the LAD code for a given LSOA code.

    Args:
        lsoa_code: The LSOA GSS code to look up.

    Returns:
        The corresponding LAD GSS code, or ``None`` if not found.
    """
    # TODO: implement
    raise NotImplementedError("lsoa_to_lad not yet implemented")
