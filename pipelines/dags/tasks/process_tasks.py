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
def process_empresas_file(reference_month: str, file_name: str, **context) -> dict:
    """
    Process a single Empresas file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    
    Args:
        reference_month: YYYY-MM format
        file_name: Name of the ZIP file
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Get force_reprocess from DAG params
    force_reprocess = context.get('params', {}).get('force_reprocess', False)
    
    # Check if already processed (skip only if not forcing reprocess)
    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
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
def process_estabelecimentos_file(reference_month: str, file_name: str, **context) -> dict:
    """
    Process a single Estabelecimentos file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    
    Args:
        reference_month: YYYY-MM format
        file_name: Name of the ZIP file
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Get force_reprocess from DAG params
    force_reprocess = context.get('params', {}).get('force_reprocess', False)
    
    # Check if already processed (skip only if not forcing reprocess)
    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
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
def transform_empresas_group():
    """
    Extract and transform all Empresas files for a given reference month.
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
    # Note: force_reprocess is read from context inside each task
    process_empresas_file.partial(
        reference_month=ref_month
    ).expand(
        file_name=files_to_process
    )


@task_group
def transform_estabelecimentos_group():
    """
    Extract and transform all Estabelecimentos files for a given reference month.
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
    # Note: force_reprocess is read from context inside each task
    process_estabelecimentos_file.partial(
        reference_month=ref_month
    ).expand(
        file_name=files_to_process
    )


# ============================================================================
# HELPER FUNCTIONS FOR LOAD TASKS
# ============================================================================

@task
def get_all_processed_months() -> list[str]:
    """
    Scan processed directory for all available months.
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
# LOAD TASK GROUPS
# ============================================================================

@task_group
def load_postgres_group():
    """
    Load existing parquet files to PostgreSQL.
    Supports specific month or 'all', and specific entity or 'all'.
    """
    @task
    def get_params(**context) -> dict:
        from .config import DEFAULT_REFERENCE_MONTH
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
            'entity_type': context['params'].get('entity_type', 'all')
        }
    
    @task(execution_timeout=None)  # UPSERT de 135M rows pode levar várias horas
    def load_to_pg(task_params: dict, **context) -> dict:
        ref_month = task_params['reference_month']
        entity_type = task_params['entity_type']
        
        months_to_process = [ref_month]
        if ref_month.lower() == 'all':
            months_to_process = get_all_processed_months.function()
            
        entities_to_process = ['empresas', 'estabelecimentos']
        if entity_type.lower() in ['empresas', 'estabelecimentos']:
            entities_to_process = [entity_type.lower()]
            
        total_loaded = 0
        
        for month in months_to_process:
            logger.info(f"Loading to Postgres for month: {month}")
            
            if 'empresas' in entities_to_process:
                empresas_files = get_parquets_for_month_and_type.function(month, 'empresas')
                if empresas_files:
                    result = load_to_postgresql.function(
                        parquet_files=empresas_files, 
                        table_name="empresa", 
                        schema="cnpj", 
                        reference_month=month,
                        **context
                    )
                    total_loaded += result.get('row_count', 0)
                    
            if 'estabelecimentos' in entities_to_process:
                estabelecimentos_files = get_parquets_for_month_and_type.function(month, 'estabelecimentos')
                if estabelecimentos_files:
                    result = load_to_postgresql.function(
                        parquet_files=estabelecimentos_files, 
                        table_name="estabelecimento", 
                        schema="cnpj", 
                        reference_month=month,
                        **context
                    )
                    total_loaded += result.get('row_count', 0)
                    
        return {"total_loaded": total_loaded}
        
    params = get_params()
    load_to_pg(params)


@task_group
def load_neo4j_group():
    """
    Load existing parquet files to Neo4j.
    Supports specific month or 'all', and specific entity or 'all'.
    """
    @task
    def get_params(**context) -> dict:
        from .config import DEFAULT_REFERENCE_MONTH
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
            'entity_type': context['params'].get('entity_type', 'all')
        }
    
    @task
    def load_to_neo(task_params: dict, **context) -> dict:
        ref_month = task_params['reference_month']
        entity_type = task_params['entity_type']
        
        months_to_process = [ref_month]
        if ref_month.lower() == 'all':
            months_to_process = get_all_processed_months.function()
            
        entities_to_process = ['empresas', 'estabelecimentos']
        if entity_type.lower() in ['empresas', 'estabelecimentos']:
            entities_to_process = [entity_type.lower()]
            
        total_nodes = 0
        total_rels = 0
        
        for month in months_to_process:
            logger.info(f"Loading to Neo4j for month: {month}")
            
            if 'empresas' in entities_to_process:
                empresas_files = get_parquets_for_month_and_type.function(month, 'empresas')
                if empresas_files:
                    result = load_to_neo4j.function(
                        parquet_files=empresas_files, 
                        entity_type="Empresa",
                        **context
                    )
                    total_nodes += result.get('total_nodes', 0)
                    
            if 'estabelecimentos' in entities_to_process:
                estabelecimentos_files = get_parquets_for_month_and_type.function(month, 'estabelecimentos')
                if estabelecimentos_files:
                    result = load_to_neo4j.function(
                        parquet_files=estabelecimentos_files, 
                        entity_type="Estabelecimento",
                        **context
                    )
                    total_nodes += result.get('total_nodes', 0)
                    total_rels += result.get('total_relationships', 0)
                    
        return {"total_nodes": total_nodes, "total_relationships": total_rels}
        
    params = get_params()
    load_to_neo(params)
