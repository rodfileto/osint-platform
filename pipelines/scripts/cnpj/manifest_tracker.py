"""
CNPJ Manifest Tracker

Helper functions to track file processing status in the manifest table.
Used by the ingestion DAG to prevent reprocessing and track progress.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, List
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


def mark_loaded_postgres(
    reference_month: str,
    file_name: str,
    rows_loaded: int
) -> None:
    """
    Mark a file as loaded to PostgreSQL in the manifest.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
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
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (datetime.now(), rows_loaded, datetime.now(), 
                  reference_month, file_name))
            conn.commit()
            logger.info(f"Marked {file_name} as loaded to PostgreSQL: {rows_loaded:,} rows")
    finally:
        conn.close()


def mark_loaded_neo4j(
    reference_month: str,
    file_name: str,
    rows_loaded: int
) -> None:
    """
    Mark a file as loaded to Neo4j in the manifest.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_name: Name of the ZIP file
        rows_loaded: Number of nodes/relationships created in Neo4j
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cnpj.download_manifest
                SET loaded_neo4j_at = %s,
                    rows_loaded_neo4j = %s,
                    processing_status = 'loaded',
                    updated_at = %s
                WHERE reference_month = %s 
                    AND file_name = %s
            """, (datetime.now(), rows_loaded, datetime.now(), 
                  reference_month, file_name))
            conn.commit()
            logger.info(f"Marked {file_name} as loaded to Neo4j: {rows_loaded:,} nodes")
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


def get_files_to_process(
    reference_month: str,
    file_type: Optional[str] = None,
    stage: str = 'extracted'
) -> List[Dict]:
    """
    Get list of files that need processing for a specific stage.
    
    Args:
        reference_month: Month in YYYY-MM format
        file_type: Filter by file type ('empresas', 'estabelecimentos', 'socios', 'reference')
        stage: Stage to check for ('extracted', 'transformed', 'loaded_postgres')
        
    Returns:
        List of file dictionaries that need processing
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            stage_column = f"{stage}_at"
            
            query = f"""
                SELECT 
                    id,
                    reference_month,
                    file_name,
                    file_type,
                    file_size_bytes,
                    csv_path,
                    parquet_path
                FROM cnpj.download_manifest
                WHERE reference_month = %s
                    AND download_date IS NOT NULL
                    AND {stage_column} IS NULL
                    AND processing_status NOT IN ('loaded')
            """
            
            params = [reference_month]
            
            if file_type:
                query += " AND file_type = %s"
                params.append(file_type)
            
            query += " ORDER BY file_type, file_name"
            
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_processing_summary(reference_month: str) -> Dict:
    """
    Get processing summary for a specific month.
    
    Args:
        reference_month: Month in YYYY-MM format
        
    Returns:
        Dictionary with processing statistics
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN download_date IS NOT NULL THEN 1 END) as downloaded,
                    COUNT(CASE WHEN extracted_at IS NOT NULL THEN 1 END) as extracted,
                    COUNT(CASE WHEN transformed_at IS NOT NULL THEN 1 END) as transformed,
                    COUNT(CASE WHEN loaded_postgres_at IS NOT NULL THEN 1 END) as loaded_postgres,
                    COUNT(CASE WHEN loaded_neo4j_at IS NOT NULL THEN 1 END) as loaded_neo4j,
                    COUNT(CASE WHEN processing_status = 'failed' THEN 1 END) as failed,
                    SUM(rows_transformed) as total_rows_transformed,
                    SUM(rows_loaded_postgres) as total_rows_loaded_postgres,
                    AVG(processing_duration_seconds) as avg_processing_seconds
                FROM cnpj.download_manifest
                WHERE reference_month = %s
            """, (reference_month,))
            
            return dict(cur.fetchone())
    finally:
        conn.close()


if __name__ == "__main__":
    # Test the tracker
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "summary":
            month = sys.argv[2] if len(sys.argv) > 2 else "2024-02"
            summary = get_processing_summary(month)
            print(f"\nProcessing Summary for {month}:")
            print(f"  Total files: {summary['total_files']}")
            print(f"  Downloaded: {summary['downloaded']}")
            print(f"  Extracted: {summary['extracted']}")
            print(f"  Transformed: {summary['transformed']}")
            print(f"  Loaded (PostgreSQL): {summary['loaded_postgres']}")
            print(f"  Loaded (Neo4j): {summary['loaded_neo4j']}")
            print(f"  Failed: {summary['failed']}")
            if summary['total_rows_transformed']:
                print(f"  Total rows transformed: {summary['total_rows_transformed']:,}")
            if summary['avg_processing_seconds']:
                print(f"  Avg processing time: {summary['avg_processing_seconds']:.2f}s")
        
        elif command == "to-process":
            month = sys.argv[2] if len(sys.argv) > 2 else "2024-02"
            stage = sys.argv[3] if len(sys.argv) > 3 else "transformed"
            files = get_files_to_process(month, stage=stage)
            print(f"\nFiles needing {stage} for {month}: {len(files)}")
            for f in files[:10]:
                print(f"  - {f['file_name']} ({f['file_type']})")
    else:
        print("Usage:")
        print("  python manifest_tracker.py summary [YYYY-MM]")
        print("  python manifest_tracker.py to-process [YYYY-MM] [stage]")
