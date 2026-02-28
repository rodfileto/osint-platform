"""
CNPJ Transform DAG

This DAG orchestrates the extraction and transformation of CNPJ data:
1. Extract: Unzip downloaded CNPJ files
2. Transform: Clean and validate data using DuckDB (pure SQL) to Parquet
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Import task groups from refactored modules
from tasks.process_tasks import (
    transform_empresas_group,
    transform_estabelecimentos_group,
    transform_socios_group,
    transform_simples_group,
    transform_references_group,
)
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
    description='Extract and transform CNPJ data to Parquet',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['cnpj', 'etl', 'duckdb', 'transform'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH,
        'process_empresas': True,
        'process_estabelecimentos': True,
        'process_socios': True,
        'process_simples': True,
        'process_references': True,
        'force_reprocess': False,  # Re-run already processed files
    },
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Branch based on execution mode
    def choose_execution_mode(**context):
        """
        Decide which execution path to take based on DAG params.
        """
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
            return 'end'
            
        return tasks_to_run
    
    branch = BranchPythonOperator(
        task_id='choose_execution_mode',
        python_callable=choose_execution_mode,
        provide_context=True
    )
    
    empresas = transform_empresas_group()
    estabelecimentos = transform_estabelecimentos_group()
    socios = transform_socios_group()
    simples = transform_simples_group()
    references = transform_references_group()
    
    # End marker (with trigger_rule ALL_DONE to handle branching)
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

    # Define dependencies
    start >> branch

    branch >> empresas >> end
    branch >> estabelecimentos >> end
    branch >> socios >> end
    branch >> simples >> end
    branch >> references >> end
    end >> trigger_next

if __name__ == "__main__":
    dag.test()
