"""
CNPJ Batch Controller DAG

This DAG orchestrates processing of multiple months by triggering the main
cnpj_ingestion DAG for each pending month.

Features:
- Discovers all months with downloaded files
- Triggers cnpj_ingestion DAG for each month
- Can process months in parallel (configurable)
- Proper Airflow monitoring and error handling
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cnpj_batch_controller',
    default_args=default_args,
    description='Batch process all pending CNPJ months',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'batch', 'controller'],
    params={
        'start_month': None,  # Optional: filter months >= this
        'end_month': None,    # Optional: filter months <= this
        'max_parallel': 3,    # Maximum months to process in parallel
    },
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    @task
    def get_pending_months(**context) -> list:
        """Discover all months with downloaded files ready to process."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process
        import psycopg2
        
        # Get all months
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='osint_metadata',
            user='osint_admin',
            password='osint_secure_password'
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT reference_month 
            FROM cnpj.download_manifest 
            WHERE file_type IN ('empresas', 'estabelecimentos')
            AND processing_status = 'downloaded'
            ORDER BY reference_month
        """)
        months = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # Filter to complete months with files to process
        complete_months = []
        for month in months:
            empresas = get_files_to_process(month, 'empresas', 'transformed')
            estab = get_files_to_process(month, 'estabelecimentos', 'transformed')
            
            # Only process if we have both empresas and estabelecimentos
            if len(empresas) >= 10 and len(estab) >= 10:
                complete_months.append(month)
        
        # Apply filters from params
        start_month = context['params'].get('start_month')
        end_month = context['params'].get('end_month')
        
        if start_month:
            complete_months = [m for m in complete_months if m >= start_month]
        if end_month:
            complete_months = [m for m in complete_months if m <= end_month]
        
        print(f"Found {len(complete_months)} months to process: {complete_months}")
        return complete_months
    
    pending_months = get_pending_months()
    
    @task
    def update_default_month(reference_month: str):
        """Update config.py with the reference month before triggering."""
        import re
        from pathlib import Path
        
        config_path = Path('/opt/airflow/dags/tasks/config.py')
        
        # Read config
        content = config_path.read_text()
        
        # Update reference month
        updated_content = re.sub(
            r'DEFAULT_REFERENCE_MONTH = "[^"]*"',
            f'DEFAULT_REFERENCE_MONTH = "{reference_month}"',
            content
        )
        
        # Write back
        config_path.write_text(updated_content)
        
        print(f"Updated config.py: DEFAULT_REFERENCE_MONTH = '{reference_month}'")
        return reference_month
    
    # Use dynamic task mapping to process months
    # The .partial() sets common params, .expand() creates one task per month
    update_config_tasks = update_default_month.partial().expand(
        reference_month=pending_months
    )
    
    @task
    def trigger_ingestion(reference_month: str):
        """Trigger the main cnpj_ingestion DAG."""
        from airflow.api.common.trigger_dag import trigger_dag
        from airflow.utils.state import DagRunState
        import time
        
        # Generate unique run_id
        run_id = f"batch_{reference_month}_{int(time.time())}"
        
        print(f"Triggering cnpj_ingestion for {reference_month}")
        
        try:
            # Trigger the DAG
            dag_run = trigger_dag(
                dag_id='cnpj_ingestion',
                run_id=run_id,
                conf={'reference_month': reference_month},
                execution_date=None,
                replace_microseconds=False,
            )
            
            print(f"✓ Triggered: {dag_run.run_id}")
            return {'month': reference_month, 'run_id': run_id, 'status': 'triggered'}
            
        except Exception as e:
            print(f"✗ Failed to trigger: {e}")
            raise
    
    # Trigger ingestion for each month (after config is updated)
    trigger_tasks = trigger_ingestion.partial().expand(
        reference_month=update_config_tasks
    )
    
    @task
    def summarize_batch(trigger_results: list):
        """Summarize batch processing results."""
        print("\n" + "="*60)
        print("BATCH PROCESSING SUMMARY")
        print("="*60)
        print(f"Total months triggered: {len(trigger_results)}")
        
        for result in trigger_results:
            print(f"  ✓ {result['month']}: {result['run_id']}")
        
        print("\nNote: Check individual cnpj_ingestion DAG runs for detailed status")
        return len(trigger_results)
    
    summary = summarize_batch(trigger_tasks)
    
    end = EmptyOperator(task_id='end')
    
    # Define flow
    start >> pending_months >> update_config_tasks >> trigger_tasks >> summary >> end
