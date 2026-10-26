"""
Test Connection DAG

This DAG tests connectivity to PostgreSQL.
It verifies that Airflow can communicate with the data layer.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_connection',
    default_args=default_args,
    description='Test connectivity to PostgreSQL',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'connectivity'],
)


def test_postgres_connection():
    """Test PostgreSQL connection and query cnpj schema"""
    logger = logging.getLogger(__name__)
    
    try:
        # Create PostgresHook using default connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Test basic connection
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        # Test database version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL version: {version}")
        
        # Test schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('public', 'cnpj', 'sanctions', 'contracts')
        """)
        schemas = cursor.fetchall()
        logger.info(f"Available schemas: {[s[0] for s in schemas]}")
        
        # Check cnpj schema exists
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'cnpj'
            );
        """)
        cnpj_exists = cursor.fetchone()[0]
        
        if cnpj_exists:
            logger.info("✓ CNPJ schema exists")
        else:
            logger.warning("✗ CNPJ schema not found - will be created by init script")
        
        cursor.close()
        connection.close()
        
        logger.info("✓ PostgreSQL connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {str(e)}")
        raise


# Define tasks
test_postgres_task = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag,
)
