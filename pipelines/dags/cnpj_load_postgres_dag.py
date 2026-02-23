"""
CNPJ Load Postgres DAG

This DAG orchestrates the loading of transformed CNPJ Parquet files into PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Import task groups from refactored modules
from tasks.process_tasks import load_postgres_group
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
    dag_id='cnpj_load_postgres',
    default_args=default_args,
    description='Load CNPJ Parquet data to PostgreSQL',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'etl', 'postgres', 'load'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH, # Can be 'all'
        'entity_type': 'all', # Can be 'empresas', 'estabelecimentos', or 'all'
        'force_reprocess': False,  # Re-run already processed files (UPSERT mode)
    },
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    load_pg = load_postgres_group()
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define dependencies
    start >> load_pg >> end

if __name__ == "__main__":
    dag.test()
