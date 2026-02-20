"""
Configuration for CNPJ Ingestion Pipeline

Shared constants and paths used across all task modules.
"""

from pathlib import Path

# Environment configuration
BASE_PATH = Path("/opt/airflow/data/cnpj")
RAW_PATH = BASE_PATH / "raw"
STAGING_PATH = BASE_PATH / "staging"
PROCESSED_PATH = BASE_PATH / "processed"
TEMP_PATH = BASE_PATH / "temp"
TEMP_SSD_PATH = Path("/opt/airflow/temp_ssd")  # Mapped to fast local SSD

# Default month to process (can be overridden via DAG params)
DEFAULT_REFERENCE_MONTH = "2024-02"
