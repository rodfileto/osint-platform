#!/bin/bash
# Populate download_manifest table with existing downloaded files

set -e

echo "=== CNPJ Download Manifest Population ==="
echo "Scanning: /media/bigdata/osint-platform/data/cnpj/raw"
echo ""

# Use full docker path
DOCKER_CMD="/snap/bin/docker"

# Run Python script inside Airflow scheduler container
$DOCKER_CMD compose exec -T airflow-scheduler python3 << 'PYTHON_SCRIPT'
import os
import re
from pathlib import Path
from datetime import datetime
import sys

# Add psycopg2 for database connection
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: psycopg2 not installed in container")
    print("Installing psycopg2-binary...")
    os.system("pip install psycopg2-binary > /dev/null 2>&1")
    import psycopg2
    from psycopg2.extras import execute_values

# Database connection
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'osint_metadata'),
    'user': os.getenv('POSTGRES_USER', 'osint_admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'osint_secure_password')
}

DATA_DIR = Path('/opt/airflow/data/cnpj/raw')

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
    for pattern, file_type in FILE_TYPE_PATTERNS.items():
        if re.match(pattern, filename, re.IGNORECASE):
            return file_type
    return 'unknown'

# Scan files
files_data = []
for month_dir in sorted(DATA_DIR.iterdir()):
    if not month_dir.is_dir():
        continue
    if not re.match(r'^\d{4}-\d{2}$', month_dir.name):
        continue
    
    reference_month = month_dir.name
    print(f"Scanning {reference_month}...", end=' ')
    
    zip_files = list(month_dir.glob('*.zip'))
    
    for zip_file in zip_files:
        file_stat = zip_file.stat()
        files_data.append({
            'reference_month': reference_month,
            'file_name': zip_file.name,
            'file_type': get_file_type(zip_file.name),
            'file_size_bytes': file_stat.st_size,
            'download_date': datetime.fromtimestamp(file_stat.st_mtime),
            'processing_status': 'downloaded'
        })
    
    print(f"{len(zip_files)} files")

print(f"\nTotal: {len(files_data)} files across {len(set(f['reference_month'] for f in files_data))} months")

# Populate database
if '--dry-run' in sys.argv:
    print("\n✓ DRY RUN - Not updating database")
    sys.exit(0)

print("\nConnecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Clear existing if requested
if '--clear' in sys.argv:
    print("Clearing existing manifest data...")
    cursor.execute("DELETE FROM cnpj.download_manifest")
    conn.commit()
    print(f"Deleted {cursor.rowcount} records")

# Bulk insert
insert_data = [
    (
        f['reference_month'], f['file_name'], f['file_type'], f['file_size_bytes'],
        None, None, f['download_date'], None, f['processing_status'], None
    )
    for f in files_data
]

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

print(f"✓ Inserted/updated {len(insert_data)} records")

# Summary
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
    print(f"{row[0]}: {row[1]:2d} files, {row[2]:6.2f} GB")

cursor.execute("""
    SELECT 
        COUNT(*) as total_files,
        COUNT(DISTINCT reference_month) as total_months,
        SUM(file_size_bytes) / 1024.0 / 1024.0 / 1024.0 as total_gb
    FROM cnpj.download_manifest
""")

row = cursor.fetchone()
print(f"\n✓ Total: {row[0]} files, {row[1]} months, {row[2]:.2f} GB")

cursor.close()
conn.close()

PYTHON_SCRIPT
