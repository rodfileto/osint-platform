"""
CNPJ Data Ingestion DAG

This DAG orchestrates the complete CNPJ data pipeline:
1. Extract: Unzip downloaded CNPJ files
2. Transform: Clean and validate data using DuckDB (pure SQL, 10-100x faster than Python UDFs)
3. Load: Insert into PostgreSQL and Neo4j

Architecture:
- Task Groups per entity type (Empresas, Estabelecimentos, Socios)
- Dynamic task generation for multiple files (0-9 per entity)
- DuckDB for high-performance transformation
- Parquet intermediate format (compressed, columnar)
- Parallel processing where possible

Data Flow:
    ZIP files → CSV extraction → DuckDB SQL transformation → Parquet → PostgreSQL + Neo4j

Performance:
- DuckDB transforms ~4M rows/sec with pure SQL
- Parquet reduces storage by 70-80%
- Parallel file processing maximizes throughput
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Add scripts to path for manifest tracker
sys.path.insert(0, '/opt/airflow/scripts/cnpj')

# Environment configuration
BASE_PATH = Path("/opt/airflow/data/cnpj")
RAW_PATH = BASE_PATH / "raw"
STAGING_PATH = BASE_PATH / "staging"
PROCESSED_PATH = BASE_PATH / "processed"

# Default month to process (can be overridden via DAG params)
DEFAULT_REFERENCE_MONTH = "2024-02"

# Logging
logger = logging.getLogger(__name__)


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

@task
def extract_zip_file(zip_path: str, output_dir: str) -> dict:
    """
    Extract a single ZIP file to staging directory.
    
    Args:
        zip_path: Path to ZIP file
        output_dir: Output directory for extracted CSV
        
    Returns:
        Dict with extracted file info
    """
    import zipfile
    from pathlib import Path
    
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Extracting {zip_path.name} to {output_dir}")
    
    extracted_files = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.filelist:
            if not file_info.is_dir():
                extracted_path = output_dir / file_info.filename
                zip_ref.extract(file_info, output_dir)
                extracted_files.append(str(extracted_path))
                logger.info(f"  Extracted: {file_info.filename} ({file_info.file_size:,} bytes)")
    
    return {
        "zip_file": str(zip_path),
        "extracted_files": extracted_files,
        "output_dir": str(output_dir),
        "file_count": len(extracted_files)
    }


@task
def transform_empresas_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Empresas CSV using DuckDB pure SQL.
    
    Uses SQL cleaners from cleaners_sql.py for maximum performance.
    
    Args:
        csv_path: Path to extracted CSV file
        output_parquet: Output Parquet file path
        
    Returns:
        Dict with transformation stats
    """
    import duckdb
    from pathlib import Path
    import time
    
    # Import SQL query builder
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from cleaners_sql import build_empresas_query
    
    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Transforming {csv_path.name} with DuckDB SQL")
    start_time = time.time()
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Build and execute transformation query
    query = build_empresas_query(str(csv_path), str(output_parquet))
    con.execute(query)
    
    # Get row count
    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
    
    duration = time.time() - start_time
    throughput = row_count / duration if duration > 0 else 0
    
    con.close()
    
    logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
    
    return {
        "input_csv": str(csv_path),
        "output_parquet": str(output_parquet),
        "row_count": row_count,
        "duration_seconds": duration,
        "throughput_rows_per_sec": throughput
    }


@task
def transform_estabelecimentos_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Estabelecimentos CSV using DuckDB pure SQL.
    
    Args:
        csv_path: Path to extracted CSV file
        output_parquet: Output Parquet file path
        
    Returns:
        Dict with transformation stats
    """
    import duckdb
    from pathlib import Path
    import time
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from cleaners_sql import build_estabelecimentos_query
    
    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Transforming {csv_path.name} with DuckDB SQL")
    start_time = time.time()
    
    con = duckdb.connect()
    query = build_estabelecimentos_query(str(csv_path), str(output_parquet))
    con.execute(query)
    
    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
    duration = time.time() - start_time
    throughput = row_count / duration if duration > 0 else 0
    
    con.close()
    
    logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
    
    return {
        "input_csv": str(csv_path),
        "output_parquet": str(output_parquet),
        "row_count": row_count,
        "duration_seconds": duration,
        "throughput_rows_per_sec": throughput
    }


@task
def load_to_postgresql(parquet_files: list[str], table_name: str, schema: str = "cnpj") -> dict:
    """
    Load Parquet files into PostgreSQL using DuckDB's postgres extension.
    
    Uses DuckDB to read parquet and INSERT directly into PostgreSQL
    via the native postgres scanner — no Python row iteration needed.
    
    Args:
        parquet_files: List of Parquet file paths to load
        table_name: Target table name (empresas, estabelecimentos)
        schema: PostgreSQL schema name
        
    Returns:
        Dict with load stats
    """
    import duckdb
    import psycopg2
    import time
    import os
    
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "osint_metadata")
    pg_user = os.getenv("POSTGRES_USER", "osint_admin")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "osint_secure_password")
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to PostgreSQL {schema}.{table_name}")
    start_time = time.time()
    
    # First, ensure schema and table exist via psycopg2
    conn = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db,
        user=pg_user, password=pg_pass
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Read columns from first parquet to create table
            if parquet_files:
                duck_tmp = duckdb.connect()
                cols_info = duck_tmp.execute(
                    f"DESCRIBE SELECT * FROM '{parquet_files[0]}'"
                ).fetchall()
                duck_tmp.close()
                
                # Map DuckDB types to PostgreSQL types
                type_map = {
                    'VARCHAR': 'TEXT',
                    'BIGINT': 'BIGINT',
                    'INTEGER': 'INTEGER',
                    'DOUBLE': 'DOUBLE PRECISION',
                    'FLOAT': 'REAL',
                    'BOOLEAN': 'BOOLEAN',
                    'DATE': 'DATE',
                    'TIMESTAMP': 'TIMESTAMP',
                    'DECIMAL': 'NUMERIC',
                }
                
                columns_def = []
                for col_name, col_type, *_ in cols_info:
                    pg_type = 'TEXT'  # default
                    for duck_t, pg_t in type_map.items():
                        if duck_t in col_type.upper():
                            pg_type = pg_t
                            break
                    columns_def.append(f'"{col_name}" {pg_type}')
                
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    {', '.join(columns_def)},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reference_month VARCHAR(7)
                )
                """
                cur.execute(create_sql)
            conn.commit()
    finally:
        conn.close()
    
    # Now use DuckDB postgres extension for fast bulk loading
    total_rows = 0
    
    duck_conn = duckdb.connect()
    duck_conn.execute("INSTALL postgres; LOAD postgres;")
    
    # Attach PostgreSQL as a DuckDB database
    dsn = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_pass}"
    duck_conn.execute(f"ATTACH '{dsn}' AS pg_db (TYPE POSTGRES)")
    
    try:
        for parquet_file in parquet_files:
            file_name = Path(parquet_file).name
            logger.info(f"  Loading {file_name}")
            file_start = time.time()
            
            # Get row count first
            row_count = duck_conn.execute(
                f"SELECT COUNT(*) FROM '{parquet_file}'"
            ).fetchone()[0]
            
            if row_count == 0:
                logger.warning(f"  Empty file: {parquet_file}")
                continue
            
            # Get column names from parquet
            cols = duck_conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{parquet_file}')"
            ).fetchall()
            col_names = ', '.join(f'"{c[0]}"' for c in cols)
            
            # Direct INSERT from parquet into PostgreSQL via DuckDB
            duck_conn.execute(f"""
                INSERT INTO pg_db.{schema}.{table_name} ({col_names})
                SELECT {col_names} FROM '{parquet_file}'
            """)
            
            file_duration = time.time() - file_start
            total_rows += row_count
            logger.info(f"    Loaded {row_count:,} rows in {file_duration:.1f}s")
    
    except Exception as e:
        logger.error(f"PostgreSQL load failed: {e}")
        raise
    finally:
        duck_conn.execute("DETACH pg_db")
        duck_conn.close()
    
    duration = time.time() - start_time
    throughput = total_rows / duration if duration > 0 else 0
    
    logger.info(f"Loaded {total_rows:,} total rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
    
    return {
        "table": f"{schema}.{table_name}",
        "files_loaded": len(parquet_files),
        "total_rows": total_rows,
        "duration_seconds": duration,
        "throughput_rows_per_sec": throughput
    }


@task
def load_to_neo4j(parquet_files: list[str], entity_type: str) -> dict:
    """
    Load Parquet files into Neo4j as nodes and relationships.
    
    Args:
        parquet_files: List of Parquet file paths to load
        entity_type: Entity type (Empresa, Estabelecimento)
        
    Returns:
        Dict with load stats
    """
    import duckdb
    from neo4j import GraphDatabase
    import time
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to Neo4j as {entity_type}")
    start_time = time.time()
    
    # Neo4j connection
    import os
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    total_nodes = 0
    
    try:
        with driver.session() as session:
            for parquet_file in parquet_files:
                logger.info(f"  Loading {Path(parquet_file).name}")
                
                # Read Parquet with DuckDB
                duck_conn = duckdb.connect()
                df = duck_conn.execute(f"SELECT * FROM '{parquet_file}'").fetchdf()
                duck_conn.close()
                
                if len(df) == 0:
                    continue
                
                # Create nodes in batches
                batch_size = 1000
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    # Convert DataFrame rows to dicts
                    records = batch.to_dict('records')
                    
                    # Cypher query to create/merge nodes
                    if entity_type == "Empresa":
                        cypher = """
                        UNWIND $records AS record
                        MERGE (e:Empresa {cnpj_basico: record.cnpj_basico})
                        SET e.razao_social = record.razao_social,
                            e.natureza_juridica = record.natureza_juridica,
                            e.capital_social = record.capital_social,
                            e.porte_empresa = record.porte_empresa,
                            e.updated_at = datetime()
                        """
                    elif entity_type == "Estabelecimento":
                        cypher = """
                        UNWIND $records AS record
                        MERGE (est:Estabelecimento {
                            cnpj: record.cnpj_basico + record.cnpj_ordem + record.cnpj_dv
                        })
                        SET est.cnpj_basico = record.cnpj_basico,
                            est.nome_fantasia = record.nome_fantasia,
                            est.situacao_cadastral = record.situacao_cadastral,
                            est.municipio = record.municipio,
                            est.uf = record.uf,
                            est.updated_at = datetime()
                        
                        // Link to Empresa
                        MERGE (e:Empresa {cnpj_basico: record.cnpj_basico})
                        MERGE (est)-[:PERTENCE_A]->(e)
                        """
                    
                    session.run(cypher, records=records)
                    total_nodes += len(records)
                
                logger.info(f"    Created/updated {len(df):,} nodes")
    
    except Exception as e:
        logger.error(f"Neo4j load failed: {e}")
        raise
    finally:
        driver.close()
    
    duration = time.time() - start_time
    throughput = total_nodes / duration if duration > 0 else 0
    
    logger.info(f"Loaded {total_nodes:,} nodes in {duration:.2f}s ({throughput:,.0f} nodes/sec)")
    
    return {
        "entity_type": entity_type,
        "files_loaded": len(parquet_files),
        "total_nodes": total_nodes,
        "duration_seconds": duration,
        "throughput_nodes_per_sec": throughput
    }


# ============================================================================
# TASK GROUPS
# ============================================================================

@task
def process_empresas_file(reference_month: str, file_name: str) -> dict:
    """
    Process a single Empresas file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    """
    from pathlib import Path
    import time
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Check if already processed
    if is_file_processed(reference_month, file_name, 'transformed'):
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
        from manifest_tracker import mark_failed
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def process_empresas_group():
    """
    Process all Empresas files for a given reference month.
    Dynamically discovers files from manifest.
    """
    from airflow.decorators import task
    
    @task
    def get_ref_month(**context) -> str:
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
    
    @task
    def get_empresas_files(reference_month: str) -> list:
        """Get list of Empresas files to process from manifest."""
        from manifest_tracker import get_files_to_process
        files = get_files_to_process(reference_month, file_type='empresas', stage='transformed')
        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Empresas files to process: {file_names}")
        return file_names
    
    ref_month = get_ref_month()
    files_to_process = get_empresas_files(ref_month)
    
    # Process files dynamically based on manifest
    @task
    def process_files_dynamic(reference_month: str, file_names: list) -> list:
        """Process all files and return parquet paths (including already-transformed)."""
        results = []
        for file_name in file_names:
            result = process_empresas_file.function(reference_month, file_name)
            if result and result.get('parquet_path'):
                results.append(result['parquet_path'])
        return results
    
    parquet_files = process_files_dynamic(ref_month, files_to_process)
    
    # Also collect parquet paths for files already fully transformed (not returned by manifest)
    @task
    def collect_all_parquets(new_parquets: list, reference_month: str) -> list:
        """Combine new parquets with any already-transformed files not in manifest query."""
        from pathlib import Path
        existing = set(new_parquets)
        processed_dir = PROCESSED_PATH / reference_month
        if processed_dir.exists():
            for p in sorted(processed_dir.glob('empresas_*.parquet')):
                if str(p) not in existing:
                    existing.add(str(p))
        valid = sorted([p for p in existing if p and Path(p).exists()])
        logger.info(f"Total valid empresas parquet files: {len(valid)}")
        return valid
    
    valid_parquets = collect_all_parquets(parquet_files, ref_month)
    
    # Load all transformed files to PostgreSQL and Neo4j
    pg_load = load_to_postgresql(valid_parquets, table_name="empresa", schema="cnpj")
    neo4j_load = load_to_neo4j(valid_parquets, entity_type="Empresa")


@task
def process_estabelecimentos_file(reference_month: str, file_name: str) -> dict:
    """
    Process a single Estabelecimentos file: extract, transform, and return result.
    Updates manifest at each stage to track progress.
    """
    from pathlib import Path
    import time
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )
    
    # Check if already processed
    if is_file_processed(reference_month, file_name, 'transformed'):
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
        from manifest_tracker import mark_failed
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def process_estabelecimentos_group():
    """
    Process all Estabelecimentos files for a given reference month.
    Dynamically discovers files from manifest.
    """
    from airflow.decorators import task
    
    @task
    def get_ref_month(**context) -> str:
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)
    
    @task
    def get_estabelecimentos_files(reference_month: str) -> list:
        """Get list of Estabelecimentos files to process from manifest."""
        from manifest_tracker import get_files_to_process
        files = get_files_to_process(reference_month, file_type='estabelecimentos', stage='transformed')
        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Estabelecimentos files to process: {file_names}")
        return file_names
    
    ref_month = get_ref_month()
    files_to_process = get_estabelecimentos_files(ref_month)
    
    # Process files dynamically based on manifest
    @task
    def process_files_dynamic(reference_month: str, file_names: list) -> list:
        """Process all files and return parquet paths (including already-transformed)."""
        results = []
        for file_name in file_names:
            result = process_estabelecimentos_file.function(reference_month, file_name)
            if result and result.get('parquet_path'):
                results.append(result['parquet_path'])
        return results
    
    parquet_files = process_files_dynamic(ref_month, files_to_process)
    
    # Also collect parquet paths for files already fully transformed (not returned by manifest)
    @task
    def collect_all_parquets(new_parquets: list, reference_month: str) -> list:
        """Combine new parquets with any already-transformed files not in manifest query."""
        from pathlib import Path
        existing = set(new_parquets)
        processed_dir = PROCESSED_PATH / reference_month
        if processed_dir.exists():
            for p in sorted(processed_dir.glob('estabelecimentos_*.parquet')):
                if str(p) not in existing:
                    existing.add(str(p))
        valid = sorted([p for p in existing if p and Path(p).exists()])
        logger.info(f"Total valid estabelecimentos parquet files: {len(valid)}")
        return valid
    
    valid_parquets = collect_all_parquets(parquet_files, ref_month)
    
    pg_load = load_to_postgresql(valid_parquets, table_name="estabelecimento", schema="cnpj")
    neo4j_load = load_to_neo4j(valid_parquets, entity_type="Estabelecimento")


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
    schedule_interval='@monthly',  # Run monthly when new data is released
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
    
    # Process entity types in parallel
    empresas_results = process_empresas_group()
    estabelecimentos_results = process_estabelecimentos_group()
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define dependencies
    start >> [empresas_results, estabelecimentos_results] >> end


if __name__ == "__main__":
    dag.test()

