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
import logging
import re
import sys

# Add scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts/cnpj')

from downloader import CNPJDownloader, sync_all_months_to_minio

logger = logging.getLogger(__name__)


def _get_requested_reference_month(context) -> str | None:
    """Return a requested reference_month (YYYY-MM) from DagRun conf/params.

    If not provided (or set to 'auto'), returns None.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    params = context.get("params") or {}

    raw_value = conf.get("reference_month")
    if raw_value in (None, ""):
        raw_value = params.get("reference_month")

    if raw_value in (None, ""):
        return None

    value = str(raw_value).strip()
    if value.lower() in {"auto", "none", "null"}:
        return None

    if not re.match(r"^\d{4}-\d{2}$", value):
        raise ValueError(f"Invalid reference_month '{value}'. Expected format YYYY-MM (e.g., 2026-02).")

    return value


def populate_download_manifest(**context):
    """
    Populate the download_manifest table with all downloaded files.
    This tracks what has been downloaded for the ingestion pipeline.
    """
    import subprocess
    import os
    
    logger.info("Populating download manifest...")
    
    # Run the populate_manifest script
    env = os.environ.copy()
    # Manifest must reflect object storage, since we delete local ZIPs after upload.
    env["CNPJ_MANIFEST_SOURCE"] = "minio"
    env.setdefault("MINIO_BUCKET_RAW", "osint-raw")
    env.setdefault("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw")

    result = subprocess.run(
        ['python', '/opt/airflow/scripts/cnpj/populate_manifest.py'],
        capture_output=True,
        text=True,
        env=env,
    )
    
    if result.returncode != 0:
        logger.error(f"Failed to populate manifest: {result.stderr}")
        raise RuntimeError(f"Manifest population failed: {result.stderr}")
    
    logger.info("Manifest populated successfully")
    logger.info(result.stdout)
    
    return result.stdout


def generate_report_from_manifest(**context):
    """Generate a summary report from the download_manifest table.

    This avoids relying on local ZIP presence, since the pipeline uploads to MinIO
    and deletes local ZIPs afterwards.
    """
    import os
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB', 'osint_metadata'),
        user=os.getenv('POSTGRES_USER', 'osint_admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'osint_secure_password'),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    reference_month,
                    COUNT(*) AS file_count,
                    COALESCE(SUM(file_size_bytes), 0) AS total_bytes
                FROM cnpj.download_manifest
                GROUP BY reference_month
                ORDER BY reference_month
                """
            )
            rows = cur.fetchall()

            lines = []
            lines.append("=== CNPJ Download Report (manifest) ===")
            lines.append(f"Date: {datetime.now().isoformat(timespec='seconds')}")
            lines.append("")

            if not rows:
                lines.append("No rows found in cnpj.download_manifest")
            else:
                total_files = 0
                total_bytes = 0
                for reference_month, file_count, total_month_bytes in rows:
                    total_files += int(file_count)
                    total_bytes += int(total_month_bytes)
                    gb = float(total_month_bytes) / 1024 / 1024 / 1024
                    lines.append(f"{reference_month}: {file_count} files, {gb:.2f} GB")

                total_gb = float(total_bytes) / 1024 / 1024 / 1024
                lines.append("")
                lines.append(f"Total: {total_files} files across {len(rows)} months, {total_gb:.2f} GB")

            report = "\n".join(lines)
            logger.info(report)
            return report
    finally:
        conn.close()

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

def check_new_months(**context):
    """
    Check if there are new months available to download.
    
    Returns:
        List of new month strings
    """
    requested_month = _get_requested_reference_month(context)
    if requested_month:
        new_months = [requested_month]
        logger.info(f"reference_month override set: {requested_month}")
    else:
        downloader = CNPJDownloader()
        available_months = downloader.list_available_months()
        if not available_months:
            logger.warning("No months available on remote server")
            new_months = []
        else:
            latest_month = available_months[-1]
            try:
                status = downloader.get_month_status_in_minio(latest_month)
            except Exception as e:
                logger.warning(f"Failed to inspect MinIO status for {latest_month}: {e}")
                status = {'total_files': 0, 'missing_files': [], 'complete': False}

            if status['total_files'] == 0:
                logger.warning(f"Latest month {latest_month} returned no files from remote source")
                new_months = []
            elif status['complete']:
                logger.info(f"Latest available month already complete in MinIO: {latest_month}")
                new_months = []
            else:
                logger.info(
                    f"Latest available month incomplete in MinIO: {latest_month}. "
                    f"Missing {len(status['missing_files'])}/{status['total_files']} files: {status['missing_files']}"
                )
                new_months = [latest_month]
    
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
    downloader = CNPJDownloader()
    
    logger.info(f"Streaming month directly to MinIO: {month}")
    uploaded_files = downloader.download_month_to_minio(month, force=False)
    
    logger.info(f"Uploaded {len(uploaded_files)} files to MinIO for {month}")
    
    # Verify completion
    status = downloader.get_month_status_in_minio(month)
    if not status['complete']:
        raise ValueError(
            f"Month {month} incomplete: "
            f"{status['downloaded_files']}/{status['total_files']} files. "
            f"Missing: {status['missing_files']}"
        )
    
    return {
        'month': month,
        'files_uploaded': len(uploaded_files),
        'files_missing_after_upload': len(status['missing_files']),
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
    
    downloader = CNPJDownloader()
    results = []
    
    for month in new_months:
        try:
            uploaded_files = downloader.download_month_to_minio(month, force=False)
            
            # Verify completion
            status = downloader.get_month_status_in_minio(month)
            
            results.append({
                'month': month,
                'success': status['complete'],
                'files': len(uploaded_files),
                'missing': status['missing_files']
            })
            
            logger.info(f"Month {month}: Uploaded {len(uploaded_files)} files to MinIO")
            
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
    Verify integrity of all downloaded months stored in MinIO.
    """
    downloader = CNPJDownloader()

    requested_month = _get_requested_reference_month(context)
    if requested_month:
        months = [requested_month]
    else:
        # Prefer scoping verification to the months discovered by check_new_months.
        ti = context.get('task_instance')
        months_from_xcom = ti.xcom_pull(task_ids='check_new_months', key='new_months') if ti else None
        if months_from_xcom is not None:
            months = months_from_xcom
            if months == []:
                logger.info("No months downloaded in this run — skipping verification")
                context['task_instance'].xcom_push(key='incomplete_months', value=[])
                return []
        else:
            months = downloader.list_available_months()
    
    incomplete_months = []
    
    for month in months:
        status = downloader.get_month_status_in_minio(month)
        
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
    
    downloader = CNPJDownloader()
    results = []
    
    for month_info in incomplete_months:
        month = month_info['month']
        
        try:
            logger.info(f"Repairing month {month}: Missing {len(month_info['missing'])} files")
            
            # Re-stream only missing or mismatched files to MinIO
            uploaded_files = downloader.download_month_to_minio(month, force=False)
            
            # Verify again
            status = downloader.get_month_status_in_minio(month)
            
            results.append({
                'month': month,
                'success': status['complete'],
                'files_repaired': len(uploaded_files)
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
    description='Download new CNPJ data directly from Receita Federal to MinIO',
    schedule_interval='0 2 2 * *',  # Monthly on the 2nd at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'download', 'receita-federal'],
    params={
        # Optional override:
        # - 'auto' (default): detect and download new months
        # - 'YYYY-MM': download only that month (e.g., 2026-02)
        'reference_month': 'auto',
    },
) as dag:
    
    # Task 1: Check for new months
    check_new_months_task = PythonOperator(
        task_id='check_new_months',
        python_callable=check_new_months,
    )
    
    # Task 2: Download new months
    download_new_months = PythonOperator(
        task_id='download_new_months',
        python_callable=download_new_months_task,
        execution_timeout=timedelta(hours=4),
    )
    
    # Task 3: Verify all downloads
    verify_downloads = PythonOperator(
        task_id='verify_downloads',
        python_callable=verify_all_months,
    )
    
    # Task 4: Repair incomplete downloads
    repair_downloads = PythonOperator(
        task_id='repair_downloads',
        python_callable=repair_incomplete_months,
        execution_timeout=timedelta(hours=2),
    )
    
    # Task 5: Update download manifest in database
    update_manifest = PythonOperator(
        task_id='update_manifest',
        python_callable=populate_download_manifest,
    )
    
    # Task 6: Generate summary report
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report_from_manifest,
    )
    
    # Define task dependencies
    check_new_months_task >> download_new_months >> verify_downloads >> repair_downloads >> update_manifest >> generate_report


# Separate DAG for manual full sync (useful for initial setup or recovery)
with DAG(
    'cnpj_download_full_sync',
    default_args=default_args,
    description='Full sync of all CNPJ data directly to MinIO (manual trigger only)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'download', 'sync', 'manual'],
) as dag_full_sync:
    
    full_sync_task = PythonOperator(
        task_id='full_sync',
        python_callable=sync_all_months_to_minio,
        op_kwargs={'skip_existing': True},
        execution_timeout=timedelta(hours=12),
    )
    
    verify_after_sync = PythonOperator(
        task_id='verify_after_sync',
        python_callable=verify_all_months,
    )
    
    update_manifest_after_sync = PythonOperator(
        task_id='update_manifest_after_sync',
        python_callable=populate_download_manifest,
    )

    report_after_sync = PythonOperator(
        task_id='report_after_sync',
        python_callable=generate_report_from_manifest,
    )
    
    full_sync_task >> verify_after_sync >> update_manifest_after_sync >> report_after_sync
