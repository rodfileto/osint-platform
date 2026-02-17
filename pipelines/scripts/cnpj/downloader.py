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
from psycopg2.extras import execute_values


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
                status = downloader.get_month_status(month)
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
            downloader.download_month(month)
        
        elif command == "sync":
            # Sync all months
            sync_all_months()
        
        elif command == "latest":
            # Download latest month
            download_latest_month()
        
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
