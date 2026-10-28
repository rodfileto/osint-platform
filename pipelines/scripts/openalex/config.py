import os
from pathlib import Path

BUCKET_URL = "https://openalex.s3.amazonaws.com"
PREFIX = "data/parquet/"

BRONZE = Path(
    os.environ.get(
        "OPENALEX_BRONZE",
        "/mnt/data10tb/openalex_lake/bronze",
    )
)
REPO_ROOT = Path(__file__).resolve().parents[2]
LAKE_HOME = Path(os.environ.get("OPENALEX_LAKE_HOME", REPO_ROOT / "data")).expanduser()
SILVER = LAKE_HOME / "silver"
CATALOG = LAKE_HOME / "lake.duckdb"

EXCLUDED_ENTITIES = {"authors", "awards"}
