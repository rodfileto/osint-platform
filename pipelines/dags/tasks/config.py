"""Paths shared by the CNPJ pipeline task modules."""

from pathlib import Path

# Airflow temp volume (fast local disk): CSV staging, Parquet output, DuckDB spill files.
TEMP_SSD_PATH = Path("/opt/airflow/temp_ssd")
STAGING_PATH = TEMP_SSD_PATH / "cnpj_staging"
PROCESSED_PATH = TEMP_SSD_PATH / "cnpj_processed"
