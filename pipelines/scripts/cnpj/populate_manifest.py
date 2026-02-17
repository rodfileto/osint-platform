"""
Populate download_manifest table with existing downloaded files.

This script scans the CNPJ data directory and registers all existing
downloaded files in the download_manifest table for tracking.
"""

import os
import re
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'osint_metadata'),
    'user': os.getenv('POSTGRES_USER', 'osint_admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'osint_secure_password')
}

DATA_DIR = Path(os.getenv('CNPJ_DATA_DIR', '/opt/airflow/data/cnpj/raw'))

# File type patterns
FILE_TYPE_PATTERNS = {
    r'^Empresas\d+\.zip$': 'empresas',
    r'^Estabelecimentos\d+\.zip$': 'estabelecimentos',
    r'^Socios\d+\.zip$': 'socios',
    r'^Simples\.zip$': 'reference',
    r'^Cnaes\.zip$': 'reference',
    r'^Motivos\.zip$': 'reference',
    r'^Municipios\.zip$': 'reference',
    r'^Naturezas\.zip$': 'reference',
    r'^Paises\.zip$': 'reference',
    r'^Qualificacoes\.zip$': 'reference',
}


def get_file_type(filename):
    """Determine file type from filename."""
    for pattern, file_type in FILE_TYPE_PATTERNS.items():
        if re.match(pattern, filename, re.IGNORECASE):
            return file_type
    return 'unknown'


def scan_downloaded_files():
    """Scan data directory and collect file metadata."""
    files_data = []
    
    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return files_data
    
    # Iterate through month folders
    for month_dir in sorted(DATA_DIR.iterdir()):
        if not month_dir.is_dir():
            continue
        
        # Check if folder name matches YYYY-MM pattern
        if not re.match(r'^\d{4}-\d{2}$', month_dir.name):
            continue
        
        reference_month = month_dir.name
        logger.info(f"Scanning month: {reference_month}")
        
        # Scan ZIP files in this month
        zip_files = list(month_dir.glob('*.zip'))
        
        for zip_file in zip_files:
            file_stat = zip_file.stat()
            file_type = get_file_type(zip_file.name)
            
            # Get file modification time
            download_date = datetime.fromtimestamp(file_stat.st_mtime)
            
            files_data.append({
                'reference_month': reference_month,
                'file_name': zip_file.name,
                'file_type': file_type,
                'file_size_bytes': file_stat.st_size,
                'download_date': download_date,
                'processing_status': 'downloaded'
            })
        
        logger.info(f"  Found {len(zip_files)} files")
    
    return files_data


def populate_manifest(files_data, clear_existing=False):
    """Insert file metadata into download_manifest table."""
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Clear existing data if requested
        if clear_existing:
            logger.info("Clearing existing manifest data...")
            cursor.execute("DELETE FROM cnpj.download_manifest")
            conn.commit()
            logger.info(f"Deleted {cursor.rowcount} existing records")
        
        # Prepare data for bulk insert
        insert_data = [
            (
                f['reference_month'],
                f['file_name'],
                f['file_type'],
                f['file_size_bytes'],
                None,  # file_checksum
                None,  # source_url
                f['download_date'],
                None,  # last_modified_remote
                f['processing_status'],
                None   # error_message
            )
            for f in files_data
        ]
        
        # Bulk insert with ON CONFLICT to handle duplicates
        insert_query = """
            INSERT INTO cnpj.download_manifest (
                reference_month, file_name, file_type, file_size_bytes,
                file_checksum, source_url, download_date, last_modified_remote,
                processing_status, error_message
            ) VALUES %s
            ON CONFLICT (reference_month, file_name) 
            DO UPDATE SET
                file_size_bytes = EXCLUDED.file_size_bytes,
                download_date = EXCLUDED.download_date,
                processing_status = EXCLUDED.processing_status,
                updated_at = CURRENT_TIMESTAMP
        """
        
        execute_values(cursor, insert_query, insert_data)
        conn.commit()
        
        logger.info(f"Inserted/updated {len(insert_data)} records in manifest table")
        
        # Show summary statistics
        cursor.execute("""
            SELECT 
                reference_month,
                COUNT(*) as file_count,
                SUM(file_size_bytes) / 1024.0 / 1024.0 / 1024.0 as total_gb
            FROM cnpj.download_manifest
            GROUP BY reference_month
            ORDER BY reference_month
        """)
        
        print("\n=== Manifest Summary ===")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} files, {row[2]:.2f} GB")
        
        # Show totals
        cursor.execute("""
            SELECT 
                COUNT(*) as total_files,
                COUNT(DISTINCT reference_month) as total_months,
                SUM(file_size_bytes) / 1024.0 / 1024.0 / 1024.0 as total_gb
            FROM cnpj.download_manifest
        """)
        
        row = cursor.fetchone()
        print(f"\nTotal: {row[0]} files across {row[1]} months, {row[2]:.2f} GB")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Populate download_manifest with existing files')
    parser.add_argument('--clear', action='store_true', help='Clear existing manifest data before populating')
    parser.add_argument('--dry-run', action='store_true', help='Scan files but do not update database')
    
    args = parser.parse_args()
    
    logger.info("Scanning downloaded files...")
    files_data = scan_downloaded_files()
    
    logger.info(f"Found {len(files_data)} files across {len(set(f['reference_month'] for f in files_data))} months")
    
    if args.dry_run:
        logger.info("DRY RUN - Not updating database")
        
        # Show summary
        months = {}
        for f in files_data:
            month = f['reference_month']
            if month not in months:
                months[month] = {'count': 0, 'size': 0}
            months[month]['count'] += 1
            months[month]['size'] += f['file_size_bytes']
        
        print("\n=== Dry Run Summary ===")
        for month in sorted(months.keys()):
            count = months[month]['count']
            size_gb = months[month]['size'] / 1024 / 1024 / 1024
            print(f"{month}: {count} files, {size_gb:.2f} GB")
    else:
        logger.info("Populating manifest table...")
        populate_manifest(files_data, clear_existing=args.clear)
        logger.info("✓ Manifest population complete")
