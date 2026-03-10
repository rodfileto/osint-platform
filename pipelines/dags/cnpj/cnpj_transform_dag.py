"""
CNPJ Transform DAG

Optimized pipeline with parallel extraction to SSD for maximum throughput:

1. **Extract Phase**: Download all ZIPs from MinIO in parallel and extract CSVs to fast SSD
2. **Transform Phase**: Process CSVs to Parquet in parallel using SSD storage
3. **Cleanup Phase**: Remove temporary CSVs from SSD to free space

Performance optimizations:
- ZIPs stored in MinIO on slow HDD (cold storage)
- CSVs staged on fast SSD (/srv/osint, NVMe) for transformation
- **Parquet files stored on fast SSD for rapid PostgreSQL loading**
- Parallel extraction and transformation maximize throughput

Storage strategy:
- SSD is used for active working datasets (CSVs + Parquets) that are frequently read
- After PostgreSQL/Neo4j loading, parquets can optionally be archived to HDD if needed
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task, task_group

# Import task groups from refactored modules
from tasks.process_tasks import (
    transform_empresas_group,
    transform_estabelecimentos_group,
    transform_socios_group,
    transform_simples_group,
    transform_references_group,
)
from tasks.extract_tasks import extract_and_stage_file, cleanup_staging_csv, cleanup_temp_zips
from tasks.config import DEFAULT_REFERENCE_MONTH

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

with DAG(
    dag_id='cnpj_transform',
    default_args=default_args,
    description='Parallel extraction to SSD + transform CNPJ ZIPs to schema-aligned Parquet',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['cnpj', 'etl', 'duckdb', 'transform', 'minio', 'ssd-optimized'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH,
        'process_empresas': True,
        'process_estabelecimentos': True,
        'process_socios': True,
        'process_simples': True,
        'process_references': True,
        'force_reprocess': False,  # Re-run already processed files
        'parallel_extract': True,  # Enable parallel extraction to SSD
        'cleanup_csv': True,  # Clean up CSVs after transformation
    },
) as dag:
    
    # ==========================================================================
    # PHASE 1: PARALLEL EXTRACTION TO SSD
    # Extract all ZIPs from MinIO in parallel and stage CSVs on fast SSD
    # ==========================================================================
    
    @task_group(group_id='extract_phase')
    def parallel_extract_phase():
        """Extract all CNPJ ZIPs from MinIO to SSD staging in parallel."""
        
        @task
        def get_ref_month(**context) -> str:
            return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
        
        @task
        def get_files_to_extract(reference_month: str, **context) -> list:
            """Get list of all files that need extraction as list of dicts."""
            import sys
            sys.path.insert(0, '/opt/airflow/scripts/cnpj')
            from manifest_tracker import get_files_to_process, get_files_for_reprocessing
            
            params = context.get('params', {})
            force_reprocess = params.get('force_reprocess', False)
            
            all_files = []
            
            # Empresas
            if params.get('process_empresas', True):
                if force_reprocess:
                    files = get_files_for_reprocessing(reference_month, file_type='empresas', require_parquet=True)
                else:
                    files = get_files_to_process(reference_month, file_type='empresas', stage='transformed')
                all_files.extend([{'file_name': f['file_name'], 'file_type': 'empresas'} for f in files])
            
            # Estabelecimentos
            if params.get('process_estabelecimentos', True):
                if force_reprocess:
                    files = get_files_for_reprocessing(reference_month, file_type='estabelecimentos', require_parquet=True)
                else:
                    files = get_files_to_process(reference_month, file_type='estabelecimentos', stage='transformed')
                all_files.extend([{'file_name': f['file_name'], 'file_type': 'estabelecimentos'} for f in files])
            
            # Socios
            if params.get('process_socios', True):
                if force_reprocess:
                    files = get_files_for_reprocessing(reference_month, file_type='socios', require_parquet=True)
                else:
                    files = get_files_to_process(reference_month, file_type='socios', stage='transformed')
                all_files.extend([{'file_name': f['file_name'], 'file_type': 'socios'} for f in files])
            
            # Simples
            if params.get('process_simples', True):
                all_files.append({'file_name': 'Simples.zip', 'file_type': 'simples'})
            
            # References
            if params.get('process_references', True):
                ref_files = ['Cnaes.zip', 'Motivos.zip', 'Municipios.zip', 'Naturezas.zip', 'Paises.zip', 'Qualificacoes.zip']
                all_files.extend([{'file_name': fn, 'file_type': 'references'} for fn in ref_files])
            
            return all_files
        
        ref_month = get_ref_month()
        files_list = get_files_to_extract(ref_month)
        
        # Extract all files in parallel using expand_kwargs
        # This creates one task instance per file, all running in parallel
        extract_and_stage_file.partial(
            reference_month=ref_month
        ).expand_kwargs(
            files_list
        )
    
    # ==========================================================================
    # PHASE 2: TRANSFORMATION
    # Process CSVs to Parquet using existing task groups
    # ==========================================================================
    
    start = EmptyOperator(task_id='start')
    extract_done = EmptyOperator(task_id='extract_phase_complete', trigger_rule='all_done')
    
    # Branch based on execution mode
    def choose_execution_mode(**context):
        """Decide which execution path to take based on DAG params."""
        params = context.get('params', {})
        
        tasks_to_run = []
        if params.get('process_empresas', True):
            tasks_to_run.append('transform_empresas_group.get_ref_month')
        if params.get('process_estabelecimentos', True):
            tasks_to_run.append('transform_estabelecimentos_group.get_ref_month')
        if params.get('process_socios', True):
            tasks_to_run.append('transform_socios_group.get_ref_month')
        if params.get('process_simples', True):
            tasks_to_run.append('transform_simples_group.get_ref_month')
        if params.get('process_references', True):
            tasks_to_run.append('transform_references_group.get_ref_month')
            
        if not tasks_to_run:
            return 'cleanup_start'
            
        return tasks_to_run
    
    branch = BranchPythonOperator(
        task_id='choose_execution_mode',
        python_callable=choose_execution_mode,
    )
    
    empresas = transform_empresas_group()
    estabelecimentos = transform_estabelecimentos_group()
    socios = transform_socios_group()
    simples = transform_simples_group()
    references = transform_references_group()
    
    transform_done = EmptyOperator(task_id='transform_phase_complete', trigger_rule='none_failed_min_one_success')
    
    # ==========================================================================
    # PHASE 3: CLEANUP
    # Remove CSVs from SSD to free space for next run
    # ==========================================================================
    
    @task
    def get_cleanup_params(**context):
        """Get cleanup parameters."""
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
            'cleanup_enabled': context['params'].get('cleanup_csv', True)
        }
    
    cleanup_start = EmptyOperator(task_id='cleanup_start')
    
    @task.branch
    def check_cleanup_enabled(cleanup_params: dict) -> str:
        """Decide whether to run cleanup."""
        if cleanup_params.get('cleanup_enabled', True):
            return 'cleanup_staging'
        return 'cleanup_complete'
    
    cleanup_params = get_cleanup_params()
    cleanup_branch = check_cleanup_enabled(cleanup_params)
    
    @task
    def cleanup_staging(cleanup_params: dict) -> dict:
        """Clean up CSV staging and temp ZIPs."""
        ref_month = cleanup_params['reference_month']
        
        # Clean CSV staging
        csv_result = cleanup_staging_csv.function(reference_month=ref_month)
        
        # Clean temp ZIPs
        zip_result = cleanup_temp_zips.function(reference_month=ref_month)
        
        return {
            'csv_cleanup': csv_result,
            'zip_cleanup': zip_result,
            'total_freed_gb': csv_result.get('freed_gb', 0) + zip_result.get('freed_mb', 0) / 1024
        }
    
    cleanup_task = cleanup_staging(cleanup_params)
    cleanup_complete = EmptyOperator(task_id='cleanup_complete', trigger_rule='none_failed_min_one_success')
    
    # End marker
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # Encadeia automaticamente com o próximo passo do pipeline
    trigger_next = TriggerDagRunOperator(
        task_id='trigger_load_postgres',
        trigger_dag_id='cnpj_load_postgres',
        conf={
            'reference_month': '{{ params.reference_month }}',
            'entity_type': 'all',
            'force_reprocess': '{{ params.force_reprocess }}',
        },
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_timeout=None,  # sem limite — aguarda o pipeline inteiro
    )

    # ==========================================================================
    # DEPENDENCY GRAPH
    # ==========================================================================
    # Phase 1: Parallel extraction to SSD
    start >> parallel_extract_phase() >> extract_done
    
    # Phase 2: Transformation (conditional branching)
    extract_done >> branch
    
    branch >> empresas >> transform_done
    branch >> estabelecimentos >> transform_done
    branch >> socios >> transform_done
    branch >> simples >> transform_done
    branch >> references >> transform_done
    branch >> cleanup_start  # Skip to cleanup if no transforms selected
    
    # Phase 3: Cleanup
    transform_done >> cleanup_start
    cleanup_start >> cleanup_branch
    cleanup_branch >> cleanup_task >> cleanup_complete
    cleanup_branch >> cleanup_complete  # Skip path
    
    # Final phases
    cleanup_complete >> end >> trigger_next

if __name__ == "__main__":
    dag.test()
