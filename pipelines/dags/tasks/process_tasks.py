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
    def get_empresas_files(reference_month: str, **context) -> list:
        """Get list of Empresas files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process, get_files_for_reprocessing
        
        force_reprocess = context['params'].get('force_reprocess', False)
        
        if force_reprocess:
            logger.info("Force reprocess enabled - querying all files with parquets")
            files = get_files_for_reprocessing(reference_month, file_type='empresas', require_parquet=True)
        else:
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
    def get_estabelecimentos_files(reference_month: str, **context) -> list:
        """Get list of Estabelecimentos files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process, get_files_for_reprocessing
        
        force_reprocess = context['params'].get('force_reprocess', False)
        
        if force_reprocess:
            logger.info("Force reprocess enabled - querying all files with parquets")
            files = get_files_for_reprocessing(reference_month, file_type='estabelecimentos', require_parquet=True)
        else:
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


# ============================================================================
# HELPER FUNCTIONS FOR NEO4J-ONLY MODE
# ============================================================================

@task
def get_all_processed_months() -> list[str]:
    """
    Scan processed directory for all available months.
    Used when neo4j_all_months=True to process all available data.
    """
    processed_dir = PROCESSED_PATH
    if not processed_dir.exists():
        logger.warning(f"Processed directory not found: {processed_dir}")
        return []
    
    months = []
    for month_dir in sorted(processed_dir.iterdir()):
        if month_dir.is_dir() and month_dir.name.count('-') == 1:  # YYYY-MM format
            # Check if it has parquet files
            empresas_files = list(month_dir.glob("empresas_*.parquet"))
            estabelecimentos_files = list(month_dir.glob("estabelecimentos_*.parquet"))
            
            if empresas_files or estabelecimentos_files:
                months.append(month_dir.name)
                logger.info(f"  Found month: {month_dir.name} ({len(empresas_files)} empresas, {len(estabelecimentos_files)} estabelecimentos)")
    
    logger.info(f"Total processed months found: {len(months)}")
    return months


@task
def get_parquets_for_month_and_type(reference_month: str, entity_type: str) -> list[str]:
    """
    Get all parquet files for a specific month and entity type.
    Used in neo4j-only mode to load existing processed files.
    
    Args:
        reference_month: Month in YYYY-MM format
        entity_type: 'empresas' or 'estabelecimentos'
        
    Returns:
        List of parquet file paths
    """
    processed_dir = PROCESSED_PATH / reference_month
    
    if not processed_dir.exists():
        logger.warning(f"Directory not found: {processed_dir}")
        return []
    
    if entity_type.lower() == 'empresas':
        pattern = "empresas_*.parquet"
    elif entity_type.lower() == 'estabelecimentos':
        pattern = "estabelecimentos_*.parquet"
    else:
        logger.error(f"Unknown entity type: {entity_type}")
        return []
    
    parquet_files = [
        str(pq) for pq in processed_dir.glob(pattern)
        if pq.exists() and pq.stat().st_size > 0
    ]
    
    logger.info(f"Found {len(parquet_files)} {entity_type} parquet files for {reference_month}")
    return sorted(parquet_files)


# ============================================================================
# NEO4J-ONLY TASK GROUPS
# ============================================================================

@task_group
def neo4j_only_single_month_group():
    """
    Load existing parquet files to Neo4j for a single reference month.
    Skips extract/transform/PostgreSQL steps.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
    
    ref_month = get_ref_month()
    
    # Get parquets for empresas and estabelecimentos
    empresas_parquets = get_parquets_for_month_and_type(ref_month, 'empresas')
    estabelecimentos_parquets = get_parquets_for_month_and_type(ref_month, 'estabelecimentos')
    
    # Load to Neo4j
    empresas_neo4j = load_to_neo4j(empresas_parquets, entity_type="Empresa")
    estabelecimentos_neo4j = load_to_neo4j(estabelecimentos_parquets, entity_type="Estabelecimento")
    
    # Sequential execution to ensure empresas exist before estabelecimentos
    empresas_neo4j >> estabelecimentos_neo4j


@task_group
def neo4j_only_all_months_group():
    """
    Load existing parquet files to Neo4j for ALL available months.
    Scans data/cnpj/processed/ directory for all month folders.
    """
    months = get_all_processed_months()
    
    @task
    def load_all_months_to_neo4j(months: list[str]) -> dict:
        """Load all months sequentially to Neo4j."""
        if not months:
            logger.warning("No processed months found")
            return {"total_months": 0, "total_nodes": 0}
        
        total_nodes = 0
        total_rels = 0
        
        for month in months:
            logger.info(f"Processing month: {month}")
            
            # Get parquets for this month
            empresas_files = get_parquets_for_month_and_type.function(month, 'empresas')
            estabelecimentos_files = get_parquets_for_month_and_type.function(month, 'estabelecimentos')
            
            # Load empresas first
            if empresas_files:
                result = load_to_neo4j.function(empresas_files, 'Empresa')
                total_nodes += result['total_nodes']
                logger.info(f"  {month}: Loaded {result['total_nodes']:,} Empresa nodes")
            
            # Then estabelecimentos
            if estabelecimentos_files:
                result = load_to_neo4j.function(estabelecimentos_files, 'Estabelecimento')
                total_nodes += result['total_nodes']
                total_rels += result.get('total_relationships', 0)
                logger.info(f"  {month}: Loaded {result['total_nodes']:,} Estabelecimento nodes")
        
        logger.info(f"Completed loading {len(months)} months: {total_nodes:,} total nodes, {total_rels:,} relationships")
        return {
            "total_months": len(months),
            "total_nodes": total_nodes,
            "total_relationships": total_rels
        }
    
    result = load_all_months_to_neo4j(months)
    return result
