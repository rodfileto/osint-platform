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

# Import task groups from refactored modules
from tasks.process_tasks import process_empresas_group, process_estabelecimentos_group
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
    },
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Process entity types sequentially to ensure referential integrity
    # Empresas must complete before Estabelecimentos (foreign key dependency)
    empresas = process_empresas_group()
    estabelecimentos = process_estabelecimentos_group()
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define dependencies - sequential execution to avoid FK conflicts and parallel load issues
    start >> empresas >> estabelecimentos >> end


if __name__ == "__main__":
    dag.test()
