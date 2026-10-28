"""Where the OpenAlex lake reads from and writes to, from the environment.

Bronze is in MinIO (the same MINIO_* settings as the CNPJ pipeline); silver and the catalog are
on the NVMe under OPENALEX_LAKE_PATH, which the Airflow containers mount at the same path as on
the host, so the catalog's view paths work from both.
"""

import os
from dataclasses import dataclass
from pathlib import Path

UPSTREAM_PREFIX = "data/parquet/"
EXCLUDED_ENTITIES = {"authors", "awards"}


@dataclass(frozen=True)
class Lake:
    upstream_url: str
    bucket: str
    prefix: str
    home: Path
    staging: Path
    duckdb_memory_limit: str
    duckdb_temp: Path

    @classmethod
    def from_env(cls) -> "Lake":
        return cls(
            upstream_url=os.environ.get("OPENALEX_UPSTREAM_URL", "https://openalex.s3.amazonaws.com").rstrip("/"),
            bucket=os.environ.get("MINIO_BUCKET_RAW", "osint-raw"),
            prefix=os.environ.get("OPENALEX_BRONZE_PREFIX", "openalex/parquet").strip("/"),
            home=Path(os.environ.get("OPENALEX_LAKE_PATH", "/srv/osint/openalex")),
            staging=Path(os.environ.get("OPENALEX_STAGING_PATH", "/opt/airflow/temp_ssd/openalex_download")),
            duckdb_memory_limit=os.environ.get("OPENALEX_DUCKDB_MEMORY_LIMIT", "8GB"),
            duckdb_temp=Path(os.environ.get("OPENALEX_DUCKDB_TEMP_PATH", "/opt/airflow/temp_ssd/duckdb_tmp")),
        )

    @property
    def silver(self) -> Path:
        return self.home / "silver"

    @property
    def catalog(self) -> Path:
        return self.home / "lake.duckdb"

    def object_key(self, rel_path: str) -> str:
        """MinIO key of a bronze file, from its path relative to data/parquet/."""
        return f"{self.prefix}/{rel_path}"

    def s3_url(self, rel_path: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(rel_path)}"
