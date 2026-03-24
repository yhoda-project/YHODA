"""Load preprocessed wide-format CSVs into the indicator table.

Reads the YHODA team's existing preprocessed CSV files (wide format with
columns: LAD_Name, LAD_Code, <year1>, <year2>, ...) and transforms them
into the long-format Indicator schema for upserting into PostgreSQL.

Usage (from the VM)::

    export $(grep -v '^#' .env | xargs)
    uv run python -m yhovi_pipeline.utils.load_csv

Or import and call ``load_dataset()`` directly.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

from yhovi_pipeline.config import get_settings
from yhovi_pipeline.db.models import Indicator


# Map dataset codes to their indicator metadata.
# indicator_id is a short machine-readable key; indicator_name is human-readable.
DATASET_REGISTRY: dict[str, dict] = {
    "eejer": {
        "indicator_id": "employment_rate",
        "indicator_name": "Employment rate",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Employment and Jobs",
    },
    "eejse": {
        "indicator_id": "self_employment_rate",
        "indicator_name": "Self-employment rate",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Employment and Jobs",
    },
    "eejur": {
        "indicator_id": "unemployment_rate",
        "indicator_name": "Unemployment rate",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Employment and Jobs",
    },
    "eejeir": {
        "indicator_id": "econ_inactive_want_job",
        "indicator_name": "Percentage of economically inactive who want a job",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Employment and Jobs",
    },
    "eejjd": {
        "indicator_id": "jobs_per_working_age_resident",
        "indicator_name": "Number of Jobs per Working-Age Resident (16-64)",
        "unit": "ratio",
        "source": "nomis",
        "subdomain": "Employment and Jobs",
    },
    "sesq": {
        "indicator_id": "qualifications_pct",
        "indicator_name": "Percentage with qualifications",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Education and Skills",
    },
    "sesnfq": {
        "indicator_id": "no_formal_qualifications_pct",
        "indicator_name": "Percentage with no formal qualifications",
        "unit": "%",
        "source": "nomis",
        "subdomain": "Education and Skills",
    },
}


def read_wide_csv(path: str) -> pd.DataFrame:
    """Read a preprocessed wide-format CSV.

    Expected columns: LAD_Name, LAD_Code, <year1>, <year2>, ...

    Returns:
        DataFrame in wide format.
    """
    return pd.read_csv(path)


def wide_to_long(df: pd.DataFrame, dataset_code: str) -> pd.DataFrame:
    """Transform a wide-format DataFrame into the Indicator long format.

    Args:
        df: Wide DataFrame with LAD_Name, LAD_Code, and year columns.
        dataset_code: Key into DATASET_REGISTRY.

    Returns:
        Long DataFrame with columns matching the Indicator table.
    """
    meta = DATASET_REGISTRY[dataset_code]

    year_cols = [c for c in df.columns if c not in ("LAD_Name", "LAD_Code")]

    long = df.melt(
        id_vars=["LAD_Name", "LAD_Code"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )

    long["year"] = long["year"].astype(int)
    long = long.dropna(subset=["value"])

    now = datetime.utcnow()
    result = pd.DataFrame(
        {
            "indicator_id": meta["indicator_id"],
            "indicator_name": meta["indicator_name"],
            "lad_code": long["LAD_Code"],
            "lad_name": long["LAD_Name"],
            "reference_period": long["year"].apply(lambda y: date(y, 1, 1)),
            "value": long["value"].astype(float),
            "unit": meta["unit"],
            "source": meta["source"],
            "dataset_code": dataset_code,
            "created_at": now,
            "updated_at": now,
        }
    )

    return result


def load_dataset(path: str, dataset_code: str) -> int:
    """Load a single preprocessed CSV into the indicator table.

    Args:
        path: Path to the wide-format CSV file.
        dataset_code: Key into DATASET_REGISTRY.

    Returns:
        Number of rows upserted.
    """
    settings = get_settings()
    engine = create_engine(settings.database_url.get_secret_value())

    df = read_wide_csv(path)
    long = wide_to_long(df, dataset_code)

    records = long.to_dict(orient="records")
    if not records:
        print(f"  No records for {dataset_code}")
        return 0

    stmt = pg_insert(Indicator).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["indicator_id", "lad_code", "reference_period"],
        set_={
            "indicator_name": stmt.excluded.indicator_name,
            "lad_name": stmt.excluded.lad_name,
            "value": stmt.excluded.value,
            "unit": stmt.excluded.unit,
            "source": stmt.excluded.source,
            "dataset_code": stmt.excluded.dataset_code,
            "updated_at": stmt.excluded.updated_at,
        },
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    print(f"  Upserted {len(records)} rows for {dataset_code}")
    return len(records)


# Files to load: (dataset_code, relative path from data_preprocessing dir)
CSV_FILES: list[tuple[str, str]] = [
    ("eejer", "eejer/eejer_preprocessed_v4.csv"),
    ("eejse", "eejse/eejse_preprocessed_v2.csv"),
    ("eejur", "eejur/eejur_preprocessed_v2.csv"),
    ("eejeir", "eejeir/eejeir_preprocessed_v3.csv"),
    ("eejjd", "eejjd/eejjd_preprocessed_v3.csv"),
    ("sesq", "sesq/sesq_preprocessed_v1_1.csv"),
    ("sesnfq", "sesnfq/sesnfq_preprocessed_v1_1.csv"),
]

BASE_PATH = "/mnt/yhoda_drive/Shared/1_Yorkshire_Vitality_Observatory/data_preprocessing"


def load_all() -> None:
    """Load all available preprocessed CSVs into the database."""
    total = 0
    for dataset_code, rel_path in CSV_FILES:
        path = f"{BASE_PATH}/{rel_path}"
        print(f"Loading {dataset_code} from {path}...")
        try:
            count = load_dataset(path, dataset_code)
            total += count
        except Exception as e:
            print(f"  ERROR loading {dataset_code}: {e}")

    print(f"\nDone. Total rows upserted: {total}")


if __name__ == "__main__":
    load_all()
