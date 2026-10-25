"""
CNPJ Manifest Tracker

Helper functions to track file processing status in the manifest table.
Used by the ingestion DAG to prevent reprocessing and track progress.
"""

import os
import logging
from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import DictCursor


logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection for manifest queries."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'osint_metadata'),
        user=os.getenv('POSTGRES_USER', 'osint_admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'osint_secure_password')
    )


def mark_extracted(
    reference_month: str,
    file_name: str,
    csv_path: str,
    rows_extracted: Optional[int] = None
) -> None:
    """
    Mark a file as extracted in the manifest.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
        csv_path: Path to the extracted CSV file
        rows_extracted: Number of rows in the CSV (optional)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cnpj.download_manifest
                SET extracted_at = %s,
                    csv_path = %s,
                    rows_extracted = %s,
                    updated_at = %s
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (datetime.now(), csv_path, rows_extracted, datetime.now(), 
                  reference_month, file_name))
            conn.commit()
            logger.info(f"Marked {file_name} as extracted: {csv_path}")
    finally:
        conn.close()


def mark_transformed(
    reference_month: str,
    file_name: str,
    parquet_path: str,
    rows_transformed: int,
    processing_duration: Optional[float] = None
) -> None:
    """
    Mark a file as transformed in the manifest.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
        parquet_path: Path to the output Parquet file
        rows_transformed: Number of rows in the Parquet file
        processing_duration: Processing time in seconds (optional)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cnpj.download_manifest
                SET transformed_at = %s,
                    parquet_path = %s,
                    rows_transformed = %s,
                    processing_duration_seconds = %s,
                    processing_status = 'transformed',
                    updated_at = %s
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (datetime.now(), parquet_path, rows_transformed, 
                  processing_duration, datetime.now(), reference_month, file_name))
            conn.commit()
            logger.info(
                f"Marked {file_name} as transformed: {rows_transformed:,} rows "
                f"in {processing_duration:.2f}s" if processing_duration else ""
            )
    finally:
        conn.close()


def mark_loaded_postgres_by_parquet_path(
    parquet_path: str,
    rows_loaded: int
) -> None:
    """
    Mark a file as loaded to PostgreSQL in the manifest, looking up by parquet_path.

    Args:
        parquet_path: Full path to the parquet file (as stored in manifest.parquet_path)
        rows_loaded: Number of rows loaded to PostgreSQL
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cnpj.download_manifest
                SET loaded_postgres_at = %s,
                    rows_loaded_postgres = %s,
                    updated_at = %s
                WHERE parquet_path = %s
            """, (datetime.now(), rows_loaded, datetime.now(), parquet_path))
            rows_affected = cur.rowcount
            conn.commit()
            if rows_affected > 0:
                from pathlib import Path as _Path
                logger.info(f"Marked {_Path(parquet_path).name} as loaded to PostgreSQL: {rows_loaded:,} rows")
            else:
                logger.warning(f"No manifest entry found for parquet_path={parquet_path}")
    finally:
        conn.close()


def mark_failed(
    reference_month: str,
    file_name: str,
    error_message: str
) -> None:
    """
    Mark a file as failed in the manifest.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
        error_message: Error description
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cnpj.download_manifest
                SET processing_status = 'failed',
                    error_message = %s,
                    updated_at = %s
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (error_message, datetime.now(), reference_month, file_name))
            conn.commit()
            logger.error(f"Marked {file_name} as failed: {error_message}")
    finally:
        conn.close()


def is_file_processed(
    reference_month: str,
    file_name: str,
    stage: str = 'transformed'
) -> bool:
    """
    Check if a file has already been processed to a specific stage.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
        stage: Stage to check ('extracted', 'transformed', 'loaded_postgres', 'loaded_neo4j')
        
    Returns:
        True if file has been processed to this stage, False otherwise
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            stage_column = f"{stage}_at"
            cur.execute(f"""
                SELECT {stage_column} IS NOT NULL as processed
                FROM cnpj.download_manifest
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (reference_month, file_name))
            
            result = cur.fetchone()
            if result:
                return result['processed']
            return False
    finally:
        conn.close()
