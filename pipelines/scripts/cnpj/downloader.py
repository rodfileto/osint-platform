"""
CNPJ Data Downloader from Receita Federal

This module downloads CNPJ data files from the official Receita Federal repository
using WebDAV protocol. It supports incremental downloads, resume capabilities,
and parallel downloads for efficiency.

Website: https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9
"""

import os
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
import psycopg2


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


class _ProgressStream:
    def __init__(self, raw_stream, progress_bar=None):
        self._raw_stream = raw_stream
        self._progress_bar = progress_bar

    def read(self, size=-1):
        chunk = self._raw_stream.read(size)
        if chunk and self._progress_bar is not None:
            self._progress_bar.update(len(chunk))
        return chunk


# Configuration
WEBDAV_BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/webdav"
SHARE_TOKEN = "YggdBLfdninEJX9"
WEBDAV_PASSWORD = ""  # Empty for public shares

# File patterns expected in each month folder
EXPECTED_FILES = {
    "empresas": r"Empresas\d+\.zip",
    "estabelecimentos": r"Estabelecimentos\d+\.zip",
    "socios": r"Socios\d+\.zip",
    "simples": r"Simples\.zip",
    "cnaes": r"Cnaes\.zip",
    "motivos": r"Motivos\.zip",
    "municipios": r"Municipios\.zip",
    "naturezas": r"Naturezas\.zip",
    "paises": r"Paises\.zip",
    "qualificacoes": r"Qualificacoes\.zip",
}

logger = logging.getLogger(__name__)


# Database connection helper
def get_db_connection():
    """Get database connection for manifest queries."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'osint_metadata'),
        user=os.getenv('POSTGRES_USER', 'osint_admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'osint_secure_password')
    )


class CNPJDownloader:
    """Download CNPJ data files from Receita Federal using WebDAV protocol."""
    
    def __init__(
        self,
        base_url: str = WEBDAV_BASE_URL,
        share_token: str = SHARE_TOKEN,
        output_dir: str = "/opt/airflow/data/cnpj/raw"
    ):
        """
        Initialize the downloader.
        
        Args:
            base_url: WebDAV base URL
            share_token: Public share token for authentication
            output_dir: Base directory to save downloaded files
        """
        self.base_url = base_url
        self.auth = HTTPBasicAuth(share_token, WEBDAV_PASSWORD)
        self.output_dir = Path(output_dir)
        self.session = requests.Session()
        self.session.auth = self.auth
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _get_minio_bucket_and_prefix() -> tuple[str, str]:
        bucket = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
        prefix = os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw").rstrip("/")
        return bucket, prefix

    def _build_minio_key(self, month: str, filename: str) -> str:
        bucket, prefix = self._get_minio_bucket_and_prefix()
        _ = bucket
        return f"{prefix}/{month}/{filename}"

    def list_files_in_minio_month(self, month: str) -> Dict[str, Dict[str, any]]:
        """List ZIP objects for a month stored in MinIO keyed by filename."""
        bucket, prefix = self._get_minio_bucket_and_prefix()
        s3 = _get_minio_s3_client()

        month_prefix = f"{prefix}/{month}/"
        files = {}
        continuation_token = None

        while True:
            kwargs = {"Bucket": bucket, "Prefix": month_prefix, "MaxKeys": 1000}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = s3.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []) or []:
                key = obj.get("Key") or ""
                if not key.lower().endswith(".zip"):
                    continue
                filename = key.rsplit("/", 1)[-1]
                files[filename] = {
                    "name": filename,
                    "key": key,
                    "size": int(obj.get("Size") or 0),
                    "last_modified": obj.get("LastModified"),
                    "month": month,
                }

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
                continue
            break

        return files
        
    def list_available_months(self) -> List[str]:
        """
        List all available month folders on the server.
        
        Returns:
            List of month strings in format 'YYYY-MM'
        """
        try:
            response = self.session.request(
                'PROPFIND',
                self.base_url + "/",
                headers={'Depth': '1'}
            )
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            namespaces = {'d': 'DAV:'}
            
            months = []
            for response_elem in root.findall('d:response', namespaces):
                href = response_elem.find('d:href', namespaces)
                if href is not None:
                    # Extract folder name from href
                    path = href.text.strip('/')
                    folder_name = path.split('/')[-1]
                    
                    # Match YYYY-MM pattern
                    if re.match(r'^\d{4}-\d{2}$', folder_name):
                        months.append(folder_name)
            
            return sorted(months)
            
        except Exception as e:
            logger.error(f"Failed to list available months: {e}")
            raise
    
    def list_files_in_month(self, month: str) -> List[Dict[str, any]]:
        """
        List all files in a specific month folder.
        
        Args:
            month: Month string in format 'YYYY-MM'
            
        Returns:
            List of dictionaries with file metadata:
                - name: filename
                - size: file size in bytes
                - modified: last modified date
                - url: download URL
        """
        try:
            url = f"{self.base_url}/{month}/"
            response = self.session.request(
                'PROPFIND',
                url,
                headers={'Depth': '1'}
            )
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            namespaces = {
                'd': 'DAV:',
                'oc': 'http://owncloud.org/ns',
                'nc': 'http://nextcloud.org/ns'
            }
            
            files = []
            for response_elem in root.findall('d:response', namespaces):
                href = response_elem.find('d:href', namespaces)
                propstat = response_elem.find('d:propstat', namespaces)
                
                if href is not None and propstat is not None:
                    prop = propstat.find('d:prop', namespaces)
                    
                    # Skip if it's a collection (directory)
                    resourcetype = prop.find('d:resourcetype', namespaces)
                    if resourcetype is not None and len(resourcetype) > 0:
                        continue
                    
                    # Extract file metadata
                    path = href.text
                    filename = path.split('/')[-1]
                    
                    size_elem = prop.find('d:getcontentlength', namespaces)
                    size = int(size_elem.text) if size_elem is not None else 0
                    
                    modified_elem = prop.find('d:getlastmodified', namespaces)
                    modified = modified_elem.text if modified_elem is not None else None
                    
                    download_url = f"https://arquivos.receitafederal.gov.br{path}"
                    
                    files.append({
                        'name': filename,
                        'size': size,
                        'modified': modified,
                        'url': download_url,
                        'month': month
                    })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files for month {month}: {e}")
            raise
    
    def download_file(
        self,
        file_info: Dict[str, any],
        force: bool = False,
        show_progress: bool = True
    ) -> Path:
        """
        Download a single file.
        
        Args:
            file_info: File metadata dictionary from list_files_in_month()
            force: If True, re-download even if file exists
            show_progress: If True, show download progress bar
            
        Returns:
            Path to downloaded file
        """
        month = file_info['month']
        filename = file_info['name']
        url = file_info['url']
        expected_size = file_info['size']
        
        # Create month directory
        month_dir = self.output_dir / month
        month_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = month_dir / filename
        
        # Check if file already exists
        if output_path.exists() and not force:
            existing_size = output_path.stat().st_size
            if existing_size == expected_size:
                logger.info(f"File already exists and matches size: {output_path}")
                return output_path
            else:
                logger.warning(
                    f"File exists but size mismatch: {existing_size} vs {expected_size}. "
                    "Re-downloading..."
                )
        
        try:
            # Download with progress bar
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as f:
                if show_progress and total_size > 0:
                    with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        desc=filename,
                        ncols=100
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            # Verify downloaded size
            downloaded_size = output_path.stat().st_size
            if downloaded_size != expected_size:
                logger.error(
                    f"Downloaded size ({downloaded_size}) doesn't match "
                    f"expected size ({expected_size})"
                )
                output_path.unlink()  # Delete incomplete file
                raise ValueError(f"Size mismatch for {filename}")
            
            logger.info(f"Successfully downloaded: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            if output_path.exists():
                output_path.unlink()
            raise

    def download_file_to_minio(
        self,
        file_info: Dict[str, any],
        force: bool = False,
        show_progress: bool = True,
    ) -> Dict[str, any]:
        """Stream a single Receita ZIP directly to MinIO without writing to local disk."""
        from botocore.exceptions import ClientError

        month = file_info['month']
        filename = file_info['name']
        url = file_info['url']
        expected_size = int(file_info['size'])

        bucket, _ = self._get_minio_bucket_and_prefix()
        key = self._build_minio_key(month, filename)

        s3 = _get_minio_s3_client()
        _ensure_bucket_exists(s3, bucket)

        if not force:
            try:
                head = s3.head_object(Bucket=bucket, Key=key)
                remote_size = int(head.get('ContentLength') or 0)
                if remote_size == expected_size:
                    logger.info(f"Object already exists and matches size: s3://{bucket}/{key}")
                    return {
                        'month': month,
                        'file_name': filename,
                        'bucket': bucket,
                        'key': key,
                        'size': remote_size,
                        'skipped': True,
                    }
                logger.warning(
                    f"Object exists but size mismatch: {remote_size} vs {expected_size}. Re-uploading {filename}"
                )
            except ClientError as e:
                code = (e.response or {}).get('Error', {}).get('Code')
                if code not in {'404', 'NoSuchKey', 'NotFound'}:
                    raise

        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            response.raw.decode_content = True

            progress_bar = None
            if show_progress and expected_size > 0:
                progress_bar = tqdm(
                    total=expected_size,
                    unit='B',
                    unit_scale=True,
                    desc=filename,
                    ncols=100,
                )

            try:
                body = _ProgressStream(response.raw, progress_bar)
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentLength=expected_size,
                )
            finally:
                if progress_bar is not None:
                    progress_bar.close()
                response.close()

            head = s3.head_object(Bucket=bucket, Key=key)
            uploaded_size = int(head.get('ContentLength') or 0)
            if uploaded_size != expected_size:
                raise ValueError(
                    f"Uploaded object size ({uploaded_size}) doesn't match expected size ({expected_size}) for {filename}"
                )

            logger.info(f"Successfully uploaded to MinIO: s3://{bucket}/{key}")
            return {
                'month': month,
                'file_name': filename,
                'bucket': bucket,
                'key': key,
                'size': uploaded_size,
                'skipped': False,
            }
        except Exception as e:
            logger.error(f"Failed to upload {filename} to MinIO: {e}")
            raise
    
    def download_month(
        self,
        month: str,
        file_types: Optional[List[str]] = None,
        force: bool = False
    ) -> List[Path]:
        """
        Download all files for a specific month.
        
        Args:
            month: Month string in format 'YYYY-MM'
            file_types: List of file types to download (e.g., ['empresas', 'estabelecimentos'])
                       If None, downloads all files
            force: If True, re-download even if files exist
            
        Returns:
            List of paths to downloaded files
        """
        logger.info(f"Downloading files for month: {month}")
        
        # List all files in the month
        files = self.list_files_in_month(month)
        
        # Filter by file types if specified
        if file_types is not None:
            patterns = [EXPECTED_FILES.get(ft, ft) for ft in file_types]
            files = [
                f for f in files
                if any(re.match(pattern, f['name'], re.IGNORECASE) for pattern in patterns)
            ]
        
        logger.info(f"Found {len(files)} files to download")
        
        # Download each file
        downloaded_paths = []
        for file_info in files:
            try:
                path = self.download_file(file_info, force=force)
                downloaded_paths.append(path)
            except Exception as e:
                logger.error(f"Failed to download {file_info['name']}: {e}")
                # Continue with next file
        
        return downloaded_paths

    def download_month_to_minio(
        self,
        month: str,
        file_types: Optional[List[str]] = None,
        force: bool = False,
    ) -> List[Dict[str, any]]:
        """Download all month files directly from Receita to MinIO."""
        logger.info(f"Streaming files directly to MinIO for month: {month}")

        files = self.list_files_in_month(month)

        if file_types is not None:
            patterns = [EXPECTED_FILES.get(ft, ft) for ft in file_types]
            files = [
                f for f in files
                if any(re.match(pattern, f['name'], re.IGNORECASE) for pattern in patterns)
            ]

        logger.info(f"Found {len(files)} files to stream to MinIO")

        uploaded_objects = []
        for file_info in files:
            try:
                result = self.download_file_to_minio(file_info, force=force)
                uploaded_objects.append(result)
            except Exception as e:
                logger.error(f"Failed to stream {file_info['name']} to MinIO: {e}")

        return uploaded_objects
    
    def find_new_months(self) -> List[str]:
        """
        Find months available on server but not yet downloaded locally.
        Uses manifest table as source of truth.
        
        Returns:
            List of new month strings in format 'YYYY-MM'
        """
        # Get available months from server
        available_months = self.list_available_months()
        
        # Query manifest table for downloaded months
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT reference_month 
                    FROM cnpj.download_manifest 
                    WHERE processing_status IN ('downloaded', 'processed')
                """)
                downloaded_months = set(row[0] for row in cur.fetchall())
            conn.close()
            
            logger.info(f"Found {len(downloaded_months)} months in manifest: {sorted(downloaded_months)}")
        except Exception as e:
            logger.warning(f"Failed to query manifest table: {e}. Falling back to filesystem check.")
            # Fallback to filesystem check if database query fails
            downloaded_months = set()
            if self.output_dir.exists():
                for item in self.output_dir.iterdir():
                    if item.is_dir() and re.match(r'^\d{4}-\d{2}$', item.name):
                        downloaded_months.add(item.name)
        
        # Find new months
        new_months = [m for m in available_months if m not in downloaded_months]
        
        logger.info(f"Found {len(new_months)} new months to download: {new_months}")
        return new_months
    
    def get_month_status(self, month: str) -> Dict[str, any]:
        """
        Check download status for a specific month.
        
        Args:
            month: Month string in format 'YYYY-MM'
            
        Returns:
            Dictionary with status information:
                - month: month string
                - total_files: total files on server
                - downloaded_files: number of files downloaded
                - missing_files: list of missing filenames
                - complete: True if all files are downloaded
        """
        # List files on server
        server_files = self.list_files_in_month(month)
        server_file_dict = {f['name']: f for f in server_files}
        
        # Check local files
        month_dir = self.output_dir / month
        if not month_dir.exists():
            return {
                'month': month,
                'total_files': len(server_files),
                'downloaded_files': 0,
                'missing_files': list(server_file_dict.keys()),
                'complete': False
            }
        
        local_files = {f.name for f in month_dir.glob('*.zip')}
        missing_files = set(server_file_dict.keys()) - local_files
        
        # Also check for size mismatches
        size_mismatch_files = []
        for filename in local_files:
            if filename in server_file_dict:
                local_size = (month_dir / filename).stat().st_size
                expected_size = server_file_dict[filename]['size']
                if local_size != expected_size:
                    size_mismatch_files.append(filename)
                    missing_files.add(filename)
        
        return {
            'month': month,
            'total_files': len(server_files),
            'downloaded_files': len(local_files) - len(size_mismatch_files),
            'missing_files': sorted(missing_files),
            'size_mismatch_files': sorted(size_mismatch_files),
            'complete': len(missing_files) == 0
        }

    def get_month_status_in_minio(self, month: str) -> Dict[str, any]:
        """Check month completeness comparing Receita source against MinIO objects."""
        server_files = self.list_files_in_month(month)
        server_file_dict = {f['name']: f for f in server_files}
        minio_files = self.list_files_in_minio_month(month)

        minio_file_names = set(minio_files.keys())
        missing_files = set(server_file_dict.keys()) - minio_file_names

        size_mismatch_files = []
        for filename, object_info in minio_files.items():
            if filename in server_file_dict:
                remote_size = int(object_info['size'])
                expected_size = int(server_file_dict[filename]['size'])
                if remote_size != expected_size:
                    size_mismatch_files.append(filename)
                    missing_files.add(filename)

        return {
            'month': month,
            'total_files': len(server_files),
            'downloaded_files': len(minio_file_names) - len(size_mismatch_files),
            'missing_files': sorted(missing_files),
            'size_mismatch_files': sorted(size_mismatch_files),
            'complete': len(missing_files) == 0,
        }


def download_latest_month(output_dir: str = "/opt/airflow/data/cnpj/raw") -> str:
    """
    Download the latest available month of CNPJ data.
    
    Args:
        output_dir: Base directory to save downloaded files
        
    Returns:
        Month string that was downloaded (format: 'YYYY-MM')
    """
    downloader = CNPJDownloader(output_dir=output_dir)
    
    # Get latest month
    months = downloader.list_available_months()
    if not months:
        raise ValueError("No months available on server")
    
    latest_month = months[-1]
    logger.info(f"Latest available month: {latest_month}")
    
    # Download it
    downloader.download_month(latest_month)
    
    return latest_month


def download_latest_month_to_minio() -> str:
    """Download the latest available month directly from Receita to MinIO."""
    downloader = CNPJDownloader()

    months = downloader.list_available_months()
    if not months:
        raise ValueError("No months available on server")

    latest_month = months[-1]
    logger.info(f"Latest available month: {latest_month}")
    downloader.download_month_to_minio(latest_month)
    return latest_month


def sync_all_months(
    output_dir: str = "/opt/airflow/data/cnpj/raw",
    skip_existing: bool = True
) -> List[str]:
    """
    Sync all available months from the server.
    
    Args:
        output_dir: Base directory to save downloaded files
        skip_existing: If True, skip months that are already complete locally
        
    Returns:
        List of months that were downloaded/updated
    """
    downloader = CNPJDownloader(output_dir=output_dir)
    
    months = downloader.list_available_months()
    logger.info(f"Found {len(months)} months on server: {months[0]} to {months[-1]}")
    
    downloaded_months = []
    
    for month in months:
        # Check status
        status = downloader.get_month_status(month)
        
        if status['complete'] and skip_existing:
            logger.info(f"Month {month} already complete, skipping")
            continue
        
        if not status['complete']:
            logger.info(
                f"Month {month}: {status['downloaded_files']}/{status['total_files']} files. "
                f"Missing: {len(status['missing_files'])}"
            )
        
        # Download
        try:
            downloader.download_month(month, force=not skip_existing)
            downloaded_months.append(month)
        except Exception as e:
            logger.error(f"Failed to download month {month}: {e}")
    
    return downloaded_months


def sync_all_months_to_minio(skip_existing: bool = True) -> List[str]:
    """Sync all available months directly from Receita to MinIO."""
    downloader = CNPJDownloader()

    months = downloader.list_available_months()
    logger.info(f"Found {len(months)} months on server: {months[0]} to {months[-1]}")

    synced_months = []

    for month in months:
        status = downloader.get_month_status_in_minio(month)

        if status['complete'] and skip_existing:
            logger.info(f"Month {month} already complete in MinIO, skipping")
            continue

        if not status['complete']:
            logger.info(
                f"Month {month}: {status['downloaded_files']}/{status['total_files']} files in MinIO. "
                f"Missing: {len(status['missing_files'])}"
            )

        try:
            downloader.download_month_to_minio(month, force=not skip_existing)
            synced_months.append(month)
        except Exception as e:
            logger.error(f"Failed to sync month {month} to MinIO: {e}")

    return synced_months


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "list":
            # List available months
            downloader = CNPJDownloader()
            months = downloader.list_available_months()
            print(f"Available months ({len(months)}):")
            for month in months:
                print(f"  - {month}")
        
        elif command == "status":
            # Show status for all months
            downloader = CNPJDownloader()
            months = downloader.list_available_months()
            
            for month in months:
                status = downloader.get_month_status_in_minio(month)
                complete_mark = "✓" if status['complete'] else "✗"
                print(
                    f"{complete_mark} {month}: "
                    f"{status['downloaded_files']}/{status['total_files']} files"
                )
                if not status['complete'] and status['missing_files']:
                    print(f"   Missing: {', '.join(status['missing_files'][:5])}")
        
        elif command == "download":
            # Download specific month
            if len(sys.argv) < 3:
                print("Usage: python downloader.py download <YYYY-MM>")
                sys.exit(1)
            
            month = sys.argv[2]
            downloader = CNPJDownloader()
            downloader.download_month_to_minio(month)
        
        elif command == "sync":
            # Sync all months
            sync_all_months_to_minio()
        
        elif command == "latest":
            # Download latest month
            download_latest_month_to_minio()
        
        else:
            print("Unknown command. Available: list, status, download, sync, latest")
            sys.exit(1)
    else:
        print("Usage: python downloader.py <command>")
        print("Commands:")
        print("  list           - List available months")
        print("  status         - Show download status for all months")
        print("  download <month> - Download specific month (YYYY-MM)")
        print("  sync           - Sync all months")
        print("  latest         - Download latest month")
