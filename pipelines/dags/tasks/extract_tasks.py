"""
CNPJ Extraction Tasks

Tasks for extracting ZIP files containing CNPJ data.
"""

import logging
import zipfile
from pathlib import Path
from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def extract_zip_file(zip_path: str, output_dir: str) -> dict:
    """
    Extract a single ZIP file to staging directory.
    
    Args:
        zip_path: Path to ZIP file
        output_dir: Output directory for extracted CSV
        
    Returns:
        Dict with extracted file info
    """
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Extracting {zip_path.name} to {output_dir}")
    
    extracted_files = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.filelist:
            if not file_info.is_dir():
                extracted_path = output_dir / file_info.filename
                zip_ref.extract(file_info, output_dir)
                extracted_files.append(str(extracted_path))
                logger.info(f"  Extracted: {file_info.filename} ({file_info.file_size:,} bytes)")
    
    return {
        "zip_file": str(zip_path),
        "extracted_files": extracted_files,
        "output_dir": str(output_dir),
        "file_count": len(extracted_files)
    }
