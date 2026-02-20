"""
CNPJ Process Tasks

High-level processing tasks and task groups for coordinating the pipeline.
"""

import logging
import time
from pathlib import Path
from airflow.decorators import task, task_group

from .extract_tasks import extract_zip_file
from .transform_tasks import transform_empresas_duckdb, transform_estabelecimentos_duckdb
from .load_tasks import load_to_postgresql, load_to_neo4j

logger = logging.getLogger(__name__)

# Environment configuration
BASE_PATH = Path("/opt/airflow/data/cnpj")
RAW_PATH = BASE_PATH / "raw"
STAGING_PATH = BASE_PATH / "staging"
PROCESSED_PATH = BASE_PATH / "processed"


@task
def process_empresas_file(reference_month: str, file_name: str) -> dict:
    """
    Process a single Empresas file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Check if already processed
    if is_file_processed(reference_month, file_name, 'transformed'):
        logger.info(f"{file_name} already transformed, skipping")
        # Return existing parquet path
        file_num = file_name.replace('Empresas', '').replace('.zip', '')
        parquet_file = PROCESSED_PATH / reference_month / f"empresas_{file_num}.parquet"
        if parquet_file.exists():
            return {"parquet_path": str(parquet_file), "skipped": True}
        return None
    
    try:
        # Extract file number from name (e.g., "Empresas0.zip" -> "0")
        file_num = file_name.replace('Empresas', '').replace('.zip', '')
        
        zip_file = RAW_PATH / reference_month / file_name
        staging_dir = STAGING_PATH / reference_month / f"empresas_{file_num}"
        parquet_file = PROCESSED_PATH / reference_month / f"empresas_{file_num}.parquet"
        
        # Check if ZIP file exists
        if not zip_file.exists():
            logger.warning(f"{file_name} not found for {reference_month}")
            return None
        
        # Extract (if not already done)
        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(str(zip_file), str(staging_dir))
            csv_path = extract_info["extracted_files"][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            # Get existing CSV path
            csv_files = list(staging_dir.glob('*.CSV')) + list(staging_dir.glob('*.EMPRECSV'))
            csv_path = str(csv_files[0]) if csv_files else None
        
        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")
        
        # Transform
        start_time = time.time()
        transform_info = transform_empresas_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time
        
        # Update manifest
        mark_transformed(
            reference_month,
            file_name,
            transform_info["output_parquet"],
            transform_info["row_count"],
            duration
        )
        
        return {
            "parquet_path": transform_info["output_parquet"],
            "row_count": transform_info["row_count"],
            "skipped": False
        }
        
    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task
def process_estabelecimentos_file(reference_month: str, file_name: str) -> dict:
    """
    Process a single Estabelecimentos file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Check if already processed
    if is_file_processed(reference_month, file_name, 'transformed'):
        logger.info(f"{file_name} already transformed, skipping")
        file_num = file_name.replace('Estabelecimentos', '').replace('.zip', '')
        parquet_file = PROCESSED_PATH / reference_month / f"estabelecimentos_{file_num}.parquet"
        if parquet_file.exists():
            return {"parquet_path": str(parquet_file), "skipped": True}
        return None
    
    try:
        file_num = file_name.replace('Estabelecimentos', '').replace('.zip', '')
        
        zip_file = RAW_PATH / reference_month / file_name
        staging_dir = STAGING_PATH / reference_month / f"estabelecimentos_{file_num}"
        parquet_file = PROCESSED_PATH / reference_month / f"estabelecimentos_{file_num}.parquet"
        
        if not zip_file.exists():
            logger.warning(f"{file_name} not found for {reference_month}")
            return None
        
        # Extract (if not already done)
        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(str(zip_file), str(staging_dir))
            csv_path = extract_info["extracted_files"][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = list(staging_dir.glob('*.CSV')) + list(staging_dir.glob('*.ESTABELE'))
            csv_path = str(csv_files[0]) if csv_files else None
        
        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")
        
        # Transform
        start_time = time.time()
        transform_info = transform_estabelecimentos_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time
        
        # Update manifest
        mark_transformed(
            reference_month,
            file_name,
            transform_info["output_parquet"],
            transform_info["row_count"],
            duration
        )
        
        return {
            "parquet_path": transform_info["output_parquet"],
            "row_count": transform_info["row_count"],
            "skipped": False
        }
        
    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def process_empresas_group():
    """
    Process all Empresas files for a given reference month.
    Dynamically discovers files from manifest.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
    
    @task
    def get_empresas_files(reference_month: str) -> list:
        """Get list of Empresas files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process
        
        files = get_files_to_process(reference_month, file_type='empresas', stage='transformed')
        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Empresas files to process: {file_names}")
        return file_names
    
    ref_month = get_ref_month()
    files_to_process = get_empresas_files(ref_month)
    
    # Process files in parallel using dynamic task mapping
    # Use .partial() for scalar args, .expand() for list args
    parallel_results = process_empresas_file.partial(
        reference_month=ref_month
    ).expand(
        file_name=files_to_process
    )
    
    @task
    def collect_parallel_results(results: list) -> list:
        """Collect parquet paths from parallel processing results."""
        parquet_paths = []
        for result in results:
            if result and result.get('parquet_path'):
                parquet_paths.append(result['parquet_path'])
        return parquet_paths
    
    parquet_files = collect_parallel_results(parallel_results)
    
    @task
    def collect_all_parquets(new_parquets: list, reference_month: str) -> list:
        """Combine new parquets with any already-transformed files not in manifest query."""
        existing = set(new_parquets)
        processed_dir = PROCESSED_PATH / reference_month
        if processed_dir.exists():
            for pq in processed_dir.glob("empresas_*.parquet"):
                if pq.exists() and pq.stat().st_size > 0:
                    existing.add(str(pq))
        valid = sorted([p for p in existing if p and Path(p).exists()])
        logger.info(f"Total valid empresas parquet files: {len(valid)}")
        return valid
    
    valid_parquets = collect_all_parquets(parquet_files, ref_month)
    
    # Load all transformed files to PostgreSQL first, then Neo4j
    pg_load = load_to_postgresql(valid_parquets, table_name="empresa", schema="cnpj", reference_month=ref_month)
    neo4j_load = load_to_neo4j(valid_parquets, entity_type="Empresa")
    
    # Define load dependencies: PostgreSQL → Neo4j
    pg_load >> neo4j_load


@task_group
def process_estabelecimentos_group():
    """
    Process all Estabelecimentos files for a given reference month.
    Dynamically discovers files from manifest.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
    
    @task
    def get_estabelecimentos_files(reference_month: str) -> list:
        """Get list of Estabelecimentos files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process
        
        files = get_files_to_process(reference_month, file_type='estabelecimentos', stage='transformed')
        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Estabelecimentos files to process: {file_names}")
        return file_names
    
    ref_month = get_ref_month()
    files_to_process = get_estabelecimentos_files(ref_month)
    
    # Process files in parallel using dynamic task mapping
    # Use .partial() for scalar args, .expand() for list args
    parallel_results = process_estabelecimentos_file.partial(
        reference_month=ref_month
    ).expand(
        file_name=files_to_process
    )
    
    @task
    def collect_parallel_results(results: list) -> list:
        """Collect parquet paths from parallel processing results."""
        parquet_paths = []
        for result in results:
            if result and result.get('parquet_path'):
                parquet_paths.append(result['parquet_path'])
        return parquet_paths
    
    parquet_files = collect_parallel_results(parallel_results)
    
    @task
    def collect_all_parquets(new_parquets: list, reference_month: str) -> list:
        """Combine new parquets with any already-transformed files not in manifest query."""
        existing = set(new_parquets)
        processed_dir = PROCESSED_PATH / reference_month
        if processed_dir.exists():
            for pq in processed_dir.glob("estabelecimentos_*.parquet"):
                if pq.exists() and pq.stat().st_size > 0:
                    existing.add(str(pq))
        valid = sorted([p for p in existing if p and Path(p).exists()])
        logger.info(f"Total valid estabelecimentos parquet files: {len(valid)}")
        return valid
    
    valid_parquets = collect_all_parquets(parquet_files, ref_month)
    
    # Load all transformed files to PostgreSQL first, then Neo4j
    pg_load = load_to_postgresql(valid_parquets, table_name="estabelecimento", schema="cnpj", reference_month=ref_month)
    neo4j_load = load_to_neo4j(valid_parquets, entity_type="Estabelecimento")
    
    # Define load dependencies: PostgreSQL → Neo4j
    pg_load >> neo4j_load
