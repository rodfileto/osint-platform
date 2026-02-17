"""
Test Connection DAG

This DAG tests connectivity to PostgreSQL and Neo4j databases.
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
    description='Test connectivity to PostgreSQL and Neo4j',
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


def test_neo4j_connection():
    """Test Neo4j connection"""
    logger = logging.getLogger(__name__)
    
    try:
        from neo4j import GraphDatabase
        import os
        
        # Get Neo4j credentials from environment
        neo4j_uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
        neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
        neo4j_password = os.getenv('NEO4J_PASSWORD', 'neo4j123')
        
        # Create driver
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            logger.info(f"Neo4j test query result: {record['test']}")
            
            # Get database info
            result = session.run("CALL dbms.components() YIELD name, versions")
            for record in result:
                logger.info(f"Neo4j component: {record['name']} - {record['versions']}")
        
        driver.close()
        logger.info("✓ Neo4j connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"✗ Neo4j connection failed: {str(e)}")
        logger.info("Note: Neo4j might need more time to start up")
        raise


# Define tasks
test_postgres_task = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag,
)

test_neo4j_task = PythonOperator(
    task_id='test_neo4j_connection',
    python_callable=test_neo4j_connection,
    dag=dag,
)

# Set task dependencies (run in parallel)
[test_postgres_task, test_neo4j_task]
