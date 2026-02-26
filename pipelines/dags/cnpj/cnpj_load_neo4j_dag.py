"""
CNPJ Load Neo4j DAG

This DAG orchestrates the loading of transformed CNPJ Parquet files into Neo4j.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Import task groups from refactored modules
from tasks.process_tasks import load_neo4j_group
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
    'execution_timeout': timedelta(hours=12),  # 66M empresas + 69M estabelecimentos estimado 6-8h
}

with DAG(
    dag_id='cnpj_load_neo4j',
    default_args=default_args,
    description='Load CNPJ Parquet data to Neo4j',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['cnpj', 'etl', 'neo4j', 'load'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH, # Can be 'all'
        'entity_type': 'all', # Can be 'empresas', 'estabelecimentos', or 'all'
        'force_reprocess': False,  # Re-run already processed files
    },
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    load_neo = load_neo4j_group()
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define dependencies
    start >> load_neo >> end

if __name__ == "__main__":
    dag.test()
