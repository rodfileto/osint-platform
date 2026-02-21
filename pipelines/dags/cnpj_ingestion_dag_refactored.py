"""
CNPJ Data Ingestion DAG - Refactored

This DAG orchestrates the complete CNPJ data pipeline:
1. Extract: Unzip downloaded CNPJ files
2. Transform: Clean and validate data using DuckDB (pure SQL, 10-100x faster than Python UDFs)
3. Load: Insert into PostgreSQL and Neo4j

Refactored Structure:
- tasks/extract_tasks.py - ZIP extraction
- tasks/transform_tasks.py - DuckDB transformation  
- tasks/load_tasks.py - PostgreSQL and Neo4j loading
- tasks/process_tasks.py - High-level processing coordination
- tasks/config.py - Shared configuration
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# Import task groups from refactored modules
from tasks.process_tasks import (
    process_empresas_group, 
    process_estabelecimentos_group,
    neo4j_only_single_month_group,
    neo4j_only_all_months_group
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
    dag_id='cnpj_ingestion',
    default_args=default_args,
    description='Ingest and process CNPJ data from Receita Federal',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'etl', 'duckdb'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH,
        'process_empresas': True,
        'process_estabelecimentos': True,
        'process_socios': False,  # Future implementation
        'force_reprocess': False,  # Re-run already processed files (UPSERT mode)
        'neo4j_only': False,  # Skip extract/transform/PostgreSQL, only load to Neo4j
        'neo4j_all_months': False,  # When neo4j_only=True, process all available months
    },
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Branch based on execution mode
    def choose_execution_mode(**context):
        """
        Decide which execution path to take based on DAG params.
        
        Returns task_id to execute next.
        """
        params = context.get('params', {})
        neo4j_only = params.get('neo4j_only', False)
        neo4j_all_months = params.get('neo4j_all_months', False)
        
        if neo4j_only:
            if neo4j_all_months:
                return 'neo4j_only_all_months_group.load_all_months_to_neo4j'
            else:
                return 'neo4j_only_single_month_group.get_ref_month'
        else:
            return 'process_empresas_group.get_ref_month'
    
    branch = BranchPythonOperator(
        task_id='choose_execution_mode',
        python_callable=choose_execution_mode,
        provide_context=True
    )
    
    # Normal processing mode: Extract → Transform → Load (PostgreSQL + Neo4j)
    # Process entity types sequentially to ensure referential integrity
    # Empresas must complete before Estabelecimentos (foreign key dependency)
    empresas = process_empresas_group()
    estabelecimentos = process_estabelecimentos_group()
    
    # Neo4j-only modes: Load existing parquets to Neo4j
    neo4j_single = neo4j_only_single_month_group()
    neo4j_all = neo4j_only_all_months_group()
    
    # End marker (with trigger_rule ALL_DONE to handle branching)
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    
    # Define dependencies
    start >> branch
    
    # Normal mode path
    branch >> empresas >> estabelecimentos >> end
    
    # Neo4j-only single month path  
    branch >> neo4j_single >> end
    
    # Neo4j-only all months path
    branch >> neo4j_all >> end


if __name__ == "__main__":
    dag.test()
