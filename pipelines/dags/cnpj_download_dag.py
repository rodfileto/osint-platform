"""
CNPJ Download DAG

This DAG monitors the Receita Federal repository for new CNPJ data releases
and automatically downloads them when available.

Schedule: Monthly on the 2nd at 2 AM (cron: 0 2 2 * *)
The official data is typically released on the 1st of each month.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import logging
import sys

# Add scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts/cnpj')

from downloader import CNPJDownloader, download_latest_month, sync_all_months

logger = logging.getLogger(__name__)


def populate_download_manifest(**context):
    """
    Populate the download_manifest table with all downloaded files.
    This tracks what has been downloaded for the ingestion pipeline.
    """
    import subprocess
    import os
    
    logger.info("Populating download manifest...")
    
    # Run the populate_manifest script
    result = subprocess.run(
        ['python', '/opt/airflow/scripts/cnpj/populate_manifest.py'],
        capture_output=True,
        text=True,
        env=os.environ.copy()
    )
    
    if result.returncode != 0:
        logger.error(f"Failed to populate manifest: {result.stderr}")
        raise RuntimeError(f"Manifest population failed: {result.stderr}")
    
    logger.info("Manifest populated successfully")
    logger.info(result.stdout)
    
    return result.stdout

# Default DAG arguments
default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

# DAG Configuration
DATA_DIR = '/opt/airflow/data/cnpj/raw'


def check_new_months(**context):
    """
    Check if there are new months available to download.
    
    Returns:
        List of new month strings
    """
    downloader = CNPJDownloader(output_dir=DATA_DIR)
    new_months = downloader.find_new_months()
    
    logger.info(f"Found {len(new_months)} new months: {new_months}")
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='new_months', value=new_months)
    
    return new_months


def download_specific_month(month: str, **context):
    """
    Download a specific month of CNPJ data.
    
    Args:
        month: Month string in format 'YYYY-MM'
    """
    downloader = CNPJDownloader(output_dir=DATA_DIR)
    
    logger.info(f"Downloading month: {month}")
    downloaded_files = downloader.download_month(month, force=False)
    
    logger.info(f"Downloaded {len(downloaded_files)} files for {month}")
    
    # Verify completion
    status = downloader.get_month_status(month)
    if not status['complete']:
        raise ValueError(
            f"Month {month} incomplete: "
            f"{status['downloaded_files']}/{status['total_files']} files. "
            f"Missing: {status['missing_files']}"
        )
    
    return {
        'month': month,
        'files_downloaded': len(downloaded_files),
        'total_size_mb': sum(f.stat().st_size for f in downloaded_files) / (1024 * 1024)
    }


def download_new_months_task(**context):
    """
    Download all new months that were detected.
    """
    # Get new months from XCom
    ti = context['task_instance']
    new_months = ti.xcom_pull(task_ids='check_new_months', key='new_months')
    
    if not new_months:
        logger.info("No new months to download")
        return []
    
    logger.info(f"Downloading {len(new_months)} new months: {new_months}")
    
    downloader = CNPJDownloader(output_dir=DATA_DIR)
    results = []
    
    for month in new_months:
        try:
            downloaded_files = downloader.download_month(month, force=False)
            
            # Verify completion
            status = downloader.get_month_status(month)
            
            results.append({
                'month': month,
                'success': status['complete'],
                'files': len(downloaded_files),
                'missing': status['missing_files']
            })
            
            logger.info(f"Month {month}: Downloaded {len(downloaded_files)} files")
            
        except Exception as e:
            logger.error(f"Failed to download month {month}: {e}")
            results.append({
                'month': month,
                'success': False,
                'error': str(e)
            })
    
    return results


def verify_all_months(**context):
    """
    Verify integrity of all downloaded months.
    """
    downloader = CNPJDownloader(output_dir=DATA_DIR)
    months = downloader.list_available_months()
    
    incomplete_months = []
    
    for month in months:
        status = downloader.get_month_status(month)
        
        if not status['complete']:
            incomplete_months.append({
                'month': month,
                'downloaded': status['downloaded_files'],
                'total': status['total_files'],
                'missing': status['missing_files']
            })
            logger.warning(
                f"Month {month} is incomplete: "
                f"{status['downloaded_files']}/{status['total_files']} files"
            )
    
    if incomplete_months:
        logger.warning(f"Found {len(incomplete_months)} incomplete months")
    else:
        logger.info(f"All {len(months)} months are complete")
    
    context['task_instance'].xcom_push(key='incomplete_months', value=incomplete_months)
    
    return incomplete_months


def repair_incomplete_months(**context):
    """
    Re-download missing files for incomplete months.
    """
    ti = context['task_instance']
    incomplete_months = ti.xcom_pull(task_ids='verify_downloads', key='incomplete_months')
    
    if not incomplete_months:
        logger.info("No incomplete months to repair")
        return []
    
    logger.info(f"Repairing {len(incomplete_months)} incomplete months")
    
    downloader = CNPJDownloader(output_dir=DATA_DIR)
    results = []
    
    for month_info in incomplete_months:
        month = month_info['month']
        
        try:
            logger.info(f"Repairing month {month}: Missing {len(month_info['missing'])} files")
            
            # Download the month (will skip existing files with correct size)
            downloaded_files = downloader.download_month(month, force=False)
            
            # Verify again
            status = downloader.get_month_status(month)
            
            results.append({
                'month': month,
                'success': status['complete'],
                'files_repaired': len(downloaded_files)
            })
            
            if status['complete']:
                logger.info(f"Month {month} repaired successfully")
            else:
                logger.error(f"Month {month} still incomplete after repair")
            
        except Exception as e:
            logger.error(f"Failed to repair month {month}: {e}")
            results.append({
                'month': month,
                'success': False,
                'error': str(e)
            })
    
    return results


# Create the DAG
with DAG(
    'cnpj_download',
    default_args=default_args,
    description='Download new CNPJ data from Receita Federal',
    schedule_interval='0 2 2 * *',  # Monthly on the 2nd at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'download', 'receita-federal'],
) as dag:
    
    # Task 1: Check for new months
    check_new_months_task = PythonOperator(
        task_id='check_new_months',
        python_callable=check_new_months,
        provide_context=True,
    )
    
    # Task 2: Download new months
    download_new_months = PythonOperator(
        task_id='download_new_months',
        python_callable=download_new_months_task,
        provide_context=True,
        execution_timeout=timedelta(hours=4),  # Large downloads can take time
    )
    
    # Task 3: Verify all downloads
    verify_downloads = PythonOperator(
        task_id='verify_downloads',
        python_callable=verify_all_months,
        provide_context=True,
    )
    
    # Task 4: Repair incomplete downloads
    repair_downloads = PythonOperator(
        task_id='repair_downloads',
        python_callable=repair_incomplete_months,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )
    
    # Task 5: Update download manifest in database
    update_manifest = PythonOperator(
        task_id='update_manifest',
        python_callable=populate_download_manifest,
        provide_context=True,
    )
    
    # Task 6: Generate summary report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command="""
        echo "=== CNPJ Download Summary ==="
        echo "Date: $(date)"
        echo ""
        echo "Directory: {{ params.data_dir }}"
        echo "Total size: $(du -sh {{ params.data_dir }} | cut -f1)"
        echo "Total ZIP files: $(find {{ params.data_dir }} -name '*.zip' | wc -l)"
        echo ""
        echo "Months available:"
        ls -d {{ params.data_dir }}/*/ | xargs -n 1 basename | sort
        echo ""
        echo "=== Latest Month Details ==="
        latest_month=$(ls -d {{ params.data_dir }}/*/ | xargs -n 1 basename | sort | tail -1)
        echo "Month: $latest_month"
        echo "Files: $(ls {{ params.data_dir }}/$latest_month/*.zip 2>/dev/null | wc -l)"
        echo "Size: $(du -sh {{ params.data_dir }}/$latest_month 2>/dev/null | cut -f1)"
        """,
        params={'data_dir': DATA_DIR},
    )
    
    # Define task dependencies
    check_new_months_task >> download_new_months >> verify_downloads >> repair_downloads >> update_manifest >> generate_report


# Separate DAG for manual full sync (useful for initial setup or recovery)
with DAG(
    'cnpj_download_full_sync',
    default_args=default_args,
    description='Full sync of all CNPJ data (manual trigger only)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'download', 'sync', 'manual'],
) as dag_full_sync:
    
    full_sync_task = PythonOperator(
        task_id='full_sync',
        python_callable=sync_all_months,
        op_kwargs={'output_dir': DATA_DIR, 'skip_existing': True},
        execution_timeout=timedelta(hours=12),  # Very long timeout for full sync
    )
    
    verify_after_sync = PythonOperator(
        task_id='verify_after_sync',
        python_callable=verify_all_months,
        provide_context=True,
    )
    
    update_manifest_after_sync = PythonOperator(
        task_id='update_manifest_after_sync',
        python_callable=populate_download_manifest,
        provide_context=True,
    )
    
    report_after_sync = BashOperator(
        task_id='report_after_sync',
        bash_command="""
        echo "=== CNPJ Full Sync Complete ==="
        echo "Date: $(date)"
        echo ""
        echo "Total size: $(du -sh {{ params.data_dir }} | cut -f1)"
        echo "Total ZIP files: $(find {{ params.data_dir }} -name '*.zip' | wc -l)"
        echo "Total months: $(ls -d {{ params.data_dir }}/*/ | wc -l)"
        echo ""
        echo "Disk usage:"
        df -h {{ params.data_dir }}
        """,
        params={'data_dir': DATA_DIR},
    )
    
    full_sync_task >> verify_after_sync >> update_manifest_after_sync >> report_after_sync
