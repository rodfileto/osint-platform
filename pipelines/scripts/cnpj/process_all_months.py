#!/usr/bin/env python3
"""
Process all pending months in the manifest.

This script triggers the CNPJ ingestion DAG for all months that have
downloaded files ready to be processed.
"""

import sys
import subprocess
import time
from datetime import datetime

sys.path.insert(0, '/opt/airflow/scripts/cnpj')

from manifest_tracker import get_files_to_process
import psycopg2


def get_pending_months():
    """Get list of months with downloaded files ready to process."""
    conn = psycopg2.connect(
        host='postgres', 
        port=5432, 
        database='osint_metadata',
        user='osint_admin',
        password='osint_secure_password'
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT reference_month 
        FROM cnpj.download_manifest 
        WHERE file_type IN ('empresas', 'estabelecimentos')
        AND processing_status = 'downloaded'
        ORDER BY reference_month
    """)
    months = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    # Filter to complete months (both empresas and estabelecimentos)
    complete_months = []
    for month in months:
        empresas = get_files_to_process(month, 'empresas', 'transformed')
        estab = get_files_to_process(month, 'estabelecimentos', 'transformed')
        
        # Only process if we have both empresas and estabelecimentos
        if len(empresas) >= 10 and len(estab) >= 10:
            complete_months.append(month)
            print(f"✓ {month}: {len(empresas)} empresas, {len(estab)} estabelecimentos")
    
    return complete_months


def trigger_dag(reference_month):
    """Trigger DAG for a specific reference month."""
    print(f"\n{'='*60}")
    print(f"Processing: {reference_month}")
    print(f"{'='*60}")
    
    # Update default reference month in config
    config_path = '/opt/airflow/dags/tasks/config.py'
    
    # Read config
    with open(config_path, 'r') as f:
        content = f.read()
    
    # Update reference month
    import re
    updated_content = re.sub(
        r'DEFAULT_REFERENCE_MONTH = "[^"]*"',
        f'DEFAULT_REFERENCE_MONTH = "{reference_month}"',
        content
    )
    
    # Write back
    with open(config_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Updated config.py with reference_month={reference_month}")
    
    # Trigger DAG test
    cmd = ['airflow', 'dags', 'test', 'cnpj_ingestion', f'{reference_month}-01']
    
    start_time = time.time()
    result = subprocess.run(cmd, capture_output=False)
    duration = time.time() - start_time
    
    if result.returncode == 0:
        print(f"✓ SUCCESS - Processed {reference_month} in {duration/60:.1f} minutes")
        return True
    else:
        print(f"✗ FAILED - {reference_month} failed with exit code {result.returncode}")
        return False


def main():
    print("="*60)
    print("CNPJ Batch Processing - All Pending Months")
    print("="*60)
    
    # Get pending months
    pending_months = get_pending_months()
    
    if not pending_months:
        print("\n✓ No months pending processing!")
        return
    
    print(f"\nFound {len(pending_months)} months ready to process")
    print(f"Estimated time: {len(pending_months) * 5:.0f}-{len(pending_months) * 15:.0f} minutes")
    
    # Confirm
    response = input(f"\nProcess all {len(pending_months)} months? [y/N]: ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Process each month
    success_count = 0
    failed_months = []
    
    overall_start = time.time()
    
    for i, month in enumerate(pending_months, 1):
        print(f"\n[{i}/{len(pending_months)}] Processing {month}...")
        
        if trigger_dag(month):
            success_count += 1
        else:
            failed_months.append(month)
    
    overall_duration = time.time() - overall_start
    
    # Summary
    print("\n" + "="*60)
    print("BATCH PROCESSING COMPLETE")
    print("="*60)
    print(f"Total time: {overall_duration/60:.1f} minutes")
    print(f"Success: {success_count}/{len(pending_months)}")
    
    if failed_months:
        print(f"\nFailed months ({len(failed_months)}):")
        for month in failed_months:
            print(f"  - {month}")
    else:
        print("\n✓ All months processed successfully!")


if __name__ == '__main__':
    main()
