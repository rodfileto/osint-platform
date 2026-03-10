"""
CNPJ Extraction Tasks

Tasks for extracting ZIP files containing CNPJ data.
"""

import logging
import os
import zipfile
from pathlib import Path
from typing import Optional

from airflow.decorators import task

logger = logging.getLogger(__name__)


def _get_minio_s3_client():
    import boto3
    from botocore.client import Config

    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials not set. Expected MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
            "(or MINIO_ACCESS_KEY/MINIO_SECRET_KEY)."
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


def _download_zip_from_minio(reference_month: str, file_name: str, temp_dir: Path) -> tuple[Path, str, str]:
    bucket = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
    base_prefix = os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw").rstrip("/")
    key = f"{base_prefix}/{reference_month}/{file_name}"

    temp_dir.mkdir(parents=True, exist_ok=True)
    local_zip_path = temp_dir / file_name

    logger.info(f"Downloading s3://{bucket}/{key} to {local_zip_path}")
    s3 = _get_minio_s3_client()
    s3.download_file(bucket, key, str(local_zip_path))

    return local_zip_path, bucket, key


@task
def extract_zip_file(
    output_dir: str,
    zip_path: Optional[str] = None,
    reference_month: Optional[str] = None,
    file_name: Optional[str] = None,
) -> dict:
    """
    Extract a single ZIP file to staging directory.

    Prefers an existing local ZIP path. If the ZIP is not present locally,
    downloads it from MinIO object storage first.

    Args:
        output_dir: Output directory for extracted CSV
        zip_path: Optional local path to ZIP file
        reference_month: Required for MinIO fallback (YYYY-MM)
        file_name: Required for MinIO fallback

    Returns:
        Dict with extracted file info
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source = "local"
    downloaded_from_minio = False

    resolved_zip_path: Optional[Path] = None
    if zip_path:
        candidate = Path(zip_path)
        if candidate.exists():
            resolved_zip_path = candidate

    if resolved_zip_path is None:
        if not reference_month or not file_name:
            raise FileNotFoundError(
                f"ZIP not found locally and MinIO lookup context missing: zip_path={zip_path!r}, "
                f"reference_month={reference_month!r}, file_name={file_name!r}"
            )

        temp_root = Path("/opt/airflow/temp_ssd")
        if not temp_root.exists():
            temp_root = Path("/opt/airflow/data/cnpj/temp")

        resolved_zip_path, bucket, key = _download_zip_from_minio(
            reference_month=reference_month,
            file_name=file_name,
            temp_dir=temp_root / "cnpj_minio_zips" / reference_month,
        )
        source = f"minio:s3://{bucket}/{key}"
        downloaded_from_minio = True

    logger.info(f"Extracting {resolved_zip_path.name} from {source} to {output_dir}")

    extracted_files = []
    try:
        with zipfile.ZipFile(resolved_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.filelist:
                if not file_info.is_dir():
                    extracted_path = output_dir / file_info.filename
                    zip_ref.extract(file_info, output_dir)
                    extracted_files.append(str(extracted_path))
                    logger.info(f"  Extracted: {file_info.filename} ({file_info.file_size:,} bytes)")
    finally:
        if downloaded_from_minio and resolved_zip_path and resolved_zip_path.exists():
            resolved_zip_path.unlink()

    return {
        "zip_file": str(resolved_zip_path),
        "zip_source": source,
        "extracted_files": extracted_files,
        "output_dir": str(output_dir),
        "file_count": len(extracted_files)
    }


@task
def extract_and_stage_file(reference_month: str, file_name: str, file_type: str) -> dict:
    """
    Download ZIP from MinIO and extract CSV to SSD staging in a single operation.
    
    Optimized for parallel execution - all files can be extracted simultaneously.
    
    Args:
        reference_month: YYYY-MM format
        file_name: ZIP file name (e.g., "Empresas0.zip")
        file_type: Entity type ('empresas', 'estabelecimentos', 'socios', etc.)
        
    Returns:
        Dict with extraction metadata
    """
    # Extract file number from name
    file_num = file_name.replace('Empresas', '').replace('Estabelecimentos', '') \
                       .replace('Socios', '').replace('Simples', '').replace('.zip', '')
    
    # Use SSD for staging (from config.py)
    from .config import TEMP_SSD_PATH
    staging_dir = TEMP_SSD_PATH / "cnpj_staging" / reference_month / f"{file_type}_{file_num}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Download ZIP to temp location on SSD
    temp_zip_dir = TEMP_SSD_PATH / "cnpj_zips_temp" / reference_month
    temp_zip_dir.mkdir(parents=True, exist_ok=True)
    
    resolved_zip_path, bucket, key = _download_zip_from_minio(
        reference_month=reference_month,
        file_name=file_name,
        temp_dir=temp_zip_dir,
    )
    
    logger.info(f"Extracting {file_name} from MinIO to SSD staging: {staging_dir}")
    
    extracted_files = []
    try:
        with zipfile.ZipFile(resolved_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.filelist:
                if not file_info.is_dir():
                    extracted_path = staging_dir / file_info.filename
                    zip_ref.extract(file_info, staging_dir)
                    extracted_files.append(str(extracted_path))
                    logger.info(f"  ✓ Extracted to SSD: {file_info.filename} ({file_info.file_size:,} bytes)")
    finally:
        # Always clean up the ZIP after extraction
        if resolved_zip_path.exists():
            resolved_zip_path.unlink()
            logger.debug(f"Cleaned up ZIP: {resolved_zip_path}")
    
    return {
        "file_name": file_name,
        "file_type": file_type,
        "reference_month": reference_month,
        "staging_dir": str(staging_dir),
        "csv_files": extracted_files,
        "file_count": len(extracted_files),
        "source": f"s3://{bucket}/{key}"
    }


@task
def cleanup_staging_csv(reference_month: str, file_type: str = None) -> dict:
    """
    Clean up CSV files from SSD staging after transformation is complete.
    
    This frees up valuable SSD space since CSVs are temporary — the source
    of truth is in MinIO (ZIPs) and the transformed output is Parquet.
    
    Args:
        reference_month: YYYY-MM format
        file_type: Optional specific type to clean ('empresas', 'estabelecimentos', etc.)
                   If None, cleans all CSV staging for the month
                   
    Returns:
        Dict with cleanup statistics
    """
    from .config import TEMP_SSD_PATH
    import shutil
    
    staging_base = TEMP_SSD_PATH / "cnpj_staging" / reference_month
    
    if not staging_base.exists():
        logger.info(f"No staging directory to clean: {staging_base}")
        return {"deleted_files": 0, "freed_bytes": 0}
    
    deleted_files = 0
    freed_bytes = 0
    
    if file_type:
        # Clean specific entity type
        pattern = f"{file_type}_*"
        dirs_to_clean = [d for d in staging_base.glob(pattern) if d.is_dir()]
    else:
        # Clean entire month
        dirs_to_clean = [staging_base]
    
    for dir_path in dirs_to_clean:
        if not dir_path.exists():
            continue
            
        # Walk directory and count files before deletion
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = Path(root) / file
                try:
                    size = file_path.stat().st_size
                    freed_bytes += size
                    deleted_files += 1
                except:
                    pass
        
        # Remove directory
        try:
            shutil.rmtree(dir_path)
            logger.info(f"✓ Cleaned up: {dir_path}")
        except Exception as e:
            logger.warning(f"Failed to clean {dir_path}: {e}")
    
    freed_mb = freed_bytes / (1024 * 1024)
    freed_gb = freed_bytes / (1024 * 1024 * 1024)
    
    logger.info(
        f"Cleanup complete: {deleted_files:,} files removed, "
        f"{freed_gb:.2f} GB freed from SSD"
    )
    
    return {
        "reference_month": reference_month,
        "file_type": file_type or "all",
        "deleted_files": deleted_files,
        "freed_bytes": freed_bytes,
        "freed_mb": round(freed_mb, 2),
        "freed_gb": round(freed_gb, 2)
    }


@task
def cleanup_temp_zips(reference_month: str = None) -> dict:
    """
    Clean up temporary ZIP download directory.
    
    ZIPs are deleted after extraction, but this ensures no orphaned files remain.
    
    Args:
        reference_month: Optional specific month. If None, cleans all temp ZIPs.
        
    Returns:
        Dict with cleanup statistics
    """
    from .config import TEMP_SSD_PATH
    import shutil
    
    temp_zip_base = TEMP_SSD_PATH / "cnpj_zips_temp"
    
    if not temp_zip_base.exists():
        logger.info("No temp ZIP directory to clean")
        return {"deleted_files": 0, "freed_bytes": 0}
    
    if reference_month:
        cleanup_path = temp_zip_base / reference_month
    else:
        cleanup_path = temp_zip_base
    
    if not cleanup_path.exists():
        logger.info(f"Nothing to clean: {cleanup_path}")
        return {"deleted_files": 0, "freed_bytes": 0}
    
    deleted_files = 0
    freed_bytes = 0
    
    for root, dirs, files in os.walk(cleanup_path):
        for file in files:
            file_path = Path(root) / file
            try:
                size = file_path.stat().st_size
                freed_bytes += size
                deleted_files += 1
            except:
                pass
    
    try:
        shutil.rmtree(cleanup_path)
        logger.info(f"✓ Cleaned up temp ZIPs: {cleanup_path}")
    except Exception as e:
        logger.warning(f"Failed to clean {cleanup_path}: {e}")
        return {"deleted_files": 0, "freed_bytes": 0, "error": str(e)}
    
    freed_mb = freed_bytes / (1024 * 1024)
    
    logger.info(f"Temp ZIP cleanup: {deleted_files:,} files, {freed_mb:.2f} MB freed")
    
    return {
        "deleted_files": deleted_files,
        "freed_bytes": freed_bytes,
        "freed_mb": round(freed_mb, 2)
    }
