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
import re
import sys
from pathlib import Path

# Add scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts/cnpj')

from downloader import CNPJDownloader, download_latest_month, sync_all_months

logger = logging.getLogger(__name__)


def _get_minio_s3_client():
    import os
    import boto3
    from botocore.client import Config

    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials not set. Expected MINIO_ROOT_USER/MINIO_ROOT_PASSWORD (or MINIO_ACCESS_KEY/MINIO_SECRET_KEY)."
        )

    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def _ensure_bucket_exists(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    s3.create_bucket(Bucket=bucket)


def _upload_file_if_needed(s3, bucket: str, key: str, file_path: Path) -> bool:
    """Upload file to S3 if object missing or size differs.

    Returns True if upload performed, False if skipped.
    """
    from botocore.exceptions import ClientError

    local_size = file_path.stat().st_size
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        remote_size = head.get("ContentLength")
        if remote_size == local_size:
            return False
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise

    s3.upload_file(str(file_path), bucket, key)
    return True


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


def _is_month_present_in_manifest(reference_month: str) -> bool:
    """Return True if any row exists for the given month in cnpj.download_manifest."""
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
                "SELECT 1 FROM cnpj.download_manifest WHERE reference_month = %s LIMIT 1",
                (reference_month,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


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


def cleanup_local_zips(**context):
    """Delete local ZIPs after successful MinIO upload.

    Deletes only the months processed in this run (or the requested month).
    """
    requested_month = _get_requested_reference_month(context)
    if requested_month:
        months = [requested_month]
    else:
        ti = context.get("task_instance")
        months_from_xcom = ti.xcom_pull(task_ids="check_new_months", key="new_months") if ti else None
        if months_from_xcom is None:
            # Task executed without upstream context (e.g., manual test)
            base_dir = Path(DATA_DIR)
            if base_dir.exists():
                local_months = sorted(
                    p.name
                    for p in base_dir.iterdir()
                    if p.is_dir() and re.match(r"^\d{4}-\d{2}$", p.name)
                )
            else:
                local_months = []
            months = [local_months[-1]] if local_months else []
        else:
            months = months_from_xcom

    if not months:
        logger.info("No months to cleanup")
        return {"deleted": 0, "months": []}

    deleted = 0
    for month in months:
        month_dir = Path(DATA_DIR) / month
        if not month_dir.exists():
            continue
        for zip_path in month_dir.glob("*.zip"):
            try:
                zip_path.unlink()
                deleted += 1
            except Exception as e:
                logger.warning(f"Failed to delete {zip_path}: {e}")

        # Remove month directory if empty
        try:
            if month_dir.exists() and not any(month_dir.iterdir()):
                month_dir.rmdir()
        except Exception:
            pass

    logger.info(f"Cleanup complete. deleted={deleted} months={months}")
    return {"deleted": deleted, "months": months}


def cleanup_all_local_zips(**context):
    """Delete all local CNPJ ZIPs (for full sync)."""
    base_dir = Path(DATA_DIR)
    if not base_dir.exists():
        return {"deleted": 0}

    deleted = 0
    for month_dir in base_dir.iterdir():
        if not month_dir.is_dir() or not re.match(r"^\d{4}-\d{2}$", month_dir.name):
            continue
        for zip_path in month_dir.glob("*.zip"):
            try:
                zip_path.unlink()
                deleted += 1
            except Exception as e:
                logger.warning(f"Failed to delete {zip_path}: {e}")
        try:
            if not any(month_dir.iterdir()):
                month_dir.rmdir()
        except Exception:
            pass

    logger.info(f"Full cleanup complete. deleted={deleted}")
    return {"deleted": deleted}


def upload_downloads_to_minio(**context):
    """Upload downloaded ZIPs to MinIO bucket `osint-raw`.

    Layout:
      s3://osint-raw/cnpj/raw/<YYYY-MM>/<filename>.zip
    """
    import os

    bucket = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
    base_prefix = os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw")

    requested_month = _get_requested_reference_month(context)
    if requested_month:
        months = [requested_month]
    else:
        ti = context.get("task_instance")
        months_from_xcom = ti.xcom_pull(task_ids="check_new_months", key="new_months") if ti else None
        if months_from_xcom is None:
            # Task executed without upstream context (e.g., `airflow tasks test`).
            base_dir = Path(DATA_DIR)
            if base_dir.exists():
                local_months = sorted(
                    p.name
                    for p in base_dir.iterdir()
                    if p.is_dir() and re.match(r"^\d{4}-\d{2}$", p.name)
                )
            else:
                local_months = []

            months = [local_months[-1]] if local_months else []
            if months:
                logger.info(f"No XCom months found; falling back to latest local month for upload: {months[0]}")
        else:
            # Normal DAG run path: respect upstream discovery (including empty list).
            months = months_from_xcom

    if not months:
        logger.info("No months to upload to MinIO")
        return {"uploaded": 0, "skipped": 0, "months": []}

    s3 = _get_minio_s3_client()
    _ensure_bucket_exists(s3, bucket)

    uploaded = 0
    skipped = 0
    for month in months:
        month_dir = Path(DATA_DIR) / month
        if not month_dir.exists():
            logger.warning(f"Month directory does not exist locally, skipping upload: {month_dir}")
            continue

        zip_files = sorted(month_dir.glob("*.zip"))
        if not zip_files:
            logger.warning(f"No ZIP files found for month {month} under {month_dir}")
            continue

        for zip_path in zip_files:
            key = f"{base_prefix}/{month}/{zip_path.name}"
            did_upload = _upload_file_if_needed(s3, bucket, key, zip_path)
            if did_upload:
                uploaded += 1
            else:
                skipped += 1

    logger.info(f"MinIO upload complete. uploaded={uploaded} skipped={skipped} bucket={bucket} prefix={base_prefix}")
    return {"uploaded": uploaded, "skipped": skipped, "months": months, "bucket": bucket, "prefix": base_prefix}


def upload_all_local_months_to_minio(**context):
    """Upload all locally present months to MinIO (for full sync)."""
    import os

    bucket = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
    base_prefix = os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw")

    base_dir = Path(DATA_DIR)
    months = []
    if base_dir.exists():
        months = sorted(
            p.name
            for p in base_dir.iterdir()
            if p.is_dir() and re.match(r"^\d{4}-\d{2}$", p.name)
        )

    if not months:
        logger.info("No local months found to upload")
        return {"uploaded": 0, "skipped": 0, "months": []}

    s3 = _get_minio_s3_client()
    _ensure_bucket_exists(s3, bucket)

    uploaded = 0
    skipped = 0
    for month in months:
        month_dir = base_dir / month
        zip_files = sorted(month_dir.glob("*.zip"))
        for zip_path in zip_files:
            key = f"{base_prefix}/{month}/{zip_path.name}"
            did_upload = _upload_file_if_needed(s3, bucket, key, zip_path)
            if did_upload:
                uploaded += 1
            else:
                skipped += 1

    logger.info(f"MinIO upload (full sync) complete. uploaded={uploaded} skipped={skipped} bucket={bucket}")
    return {"uploaded": uploaded, "skipped": skipped, "months": months, "bucket": bucket, "prefix": base_prefix}

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
    requested_month = _get_requested_reference_month(context)
    if requested_month:
        new_months = [requested_month]
        logger.info(f"reference_month override set: {requested_month}")
    else:
        downloader = CNPJDownloader(output_dir=DATA_DIR)
        available_months = downloader.list_available_months()
        if not available_months:
            logger.warning("No months available on remote server")
            new_months = []
        else:
            latest_month = available_months[-1]
            try:
                in_manifest = _is_month_present_in_manifest(latest_month)
            except Exception as e:
                logger.warning(f"Failed to query manifest for {latest_month}: {e}. Falling back to filesystem check.")
                month_dir = Path(DATA_DIR) / latest_month
                in_manifest = month_dir.exists() and any(month_dir.glob('*.zip'))

            if in_manifest:
                logger.info(f"Latest available month already present in manifest: {latest_month}")
                new_months = []
            else:
                logger.info(f"Latest available month not in manifest — will download: {latest_month}")
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
            # Fallback: verify months present locally on disk.
            base_dir = Path(DATA_DIR)
            if base_dir.exists():
                months = sorted(
                    p.name
                    for p in base_dir.iterdir()
                    if p.is_dir() and re.match(r"^\d{4}-\d{2}$", p.name)
                )
            else:
                months = []
    
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
        execution_timeout=timedelta(hours=4),  # Large downloads can take time
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
    upload_to_minio = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_downloads_to_minio,
        execution_timeout=timedelta(hours=2),
    )

    # Task 6: Update download manifest in database
    update_manifest = PythonOperator(
        task_id='update_manifest',
        python_callable=populate_download_manifest,
    )
    
    # Task 7: Generate summary report
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report_from_manifest,
    )

    cleanup_local = PythonOperator(
        task_id='cleanup_local_zips',
        python_callable=cleanup_local_zips,
        execution_timeout=timedelta(minutes=30),
    )
    
    # Define task dependencies
    check_new_months_task >> download_new_months >> verify_downloads >> repair_downloads >> upload_to_minio >> update_manifest >> generate_report >> cleanup_local


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
    )
    
    update_manifest_after_sync = PythonOperator(
        task_id='update_manifest_after_sync',
        python_callable=populate_download_manifest,
    )

    upload_to_minio_after_sync = PythonOperator(
        task_id='upload_to_minio_after_sync',
        python_callable=upload_all_local_months_to_minio,
        execution_timeout=timedelta(hours=12),
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

    cleanup_local_after_sync = PythonOperator(
        task_id='cleanup_local_zips_after_sync',
        python_callable=cleanup_all_local_zips,
        execution_timeout=timedelta(hours=2),
    )
    
    full_sync_task >> verify_after_sync >> upload_to_minio_after_sync >> update_manifest_after_sync >> report_after_sync >> cleanup_local_after_sync
