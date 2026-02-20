"""
CNPJ Load Tasks

Tasks for loading transformed data into PostgreSQL and Neo4j.
"""

import logging
import time
import os
from pathlib import Path
import duckdb
import psycopg2
from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def load_to_postgresql(parquet_files: list[str], table_name: str, schema: str = "cnpj", reference_month: str = "2024-02") -> dict:
    """
    Load Parquet files into PostgreSQL using DuckDB's postgres extension.
    
    Uses DuckDB to read parquet and INSERT directly into PostgreSQL
    via the native postgres scanner — no Python row iteration needed.
    
    Args:
        parquet_files: List of Parquet file paths to load
        table_name: Target table name (empresas, estabelecimentos)
        schema: PostgreSQL schema name
        reference_month: Reference month for the data (YYYY-MM format)
        
    Returns:
        Dict with load stats
    """
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "osint_metadata")
    pg_user = os.getenv("POSTGRES_USER", "osint_admin")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "osint_secure_password")
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to PostgreSQL {schema}.{table_name}")
    start_time = time.time()
    
    # Identify heavy indexes to drop/recreate for bulk load performance
    heavy_indexes = []
    if table_name == "empresas":
        heavy_indexes = [("idx_empresas_razao_social", "USING GIN (to_tsvector('portuguese', razao_social))")]
    elif table_name == "estabelecimentos":
        heavy_indexes = [("idx_estabelecimentos_nome_fantasia", "USING GIN (to_tsvector('portuguese', nome_fantasia))")]
    
    # First, ensure schema and table exist via psycopg2
    conn = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db,
        user=pg_user, password=pg_pass
    )
    conn.autocommit = True  # Enable autocommit for index operations
    
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
                    base_type = col_type.split('(')[0].upper()
                    pg_type = type_map.get(base_type, 'TEXT')
                    # Preserve precision for DECIMAL
                    if 'DECIMAL' in col_type.upper():
                        pg_type = col_type.replace('DECIMAL', 'NUMERIC')
                    columns_def.append(f"{col_name} {pg_type}")
                
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    {', '.join(columns_def)},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reference_month VARCHAR(7)
                )
                """
                cur.execute(create_sql)
                
            # Drop heavy indexes before loading
            for idx_name, _ in heavy_indexes:
                logger.info(f"Dropping index {schema}.{idx_name} to optimize bulk load")
                cur.execute(f"DROP INDEX IF EXISTS {schema}.{idx_name}")
    finally:
        conn.close()
    
    # Now use DuckDB postgres extension for fast bulk loading
    total_rows = 0
    
    # Optimize: Use SSD for temp storage if available
    temp_dir = Path("/opt/airflow/temp_ssd")
    if not temp_dir.exists():
        logger.warning(f"SSD temp dir {temp_dir} not found, falling back to data dir")
        temp_dir = Path("/opt/airflow/data/cnpj/temp")
    
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Create DuckDB connection with optimized memory/spilling
    duck_conn = duckdb.connect(config={'temp_directory': str(temp_dir), 'memory_limit': '5GB'})
    
    # Set NULL byte replacement to strip them (PostgreSQL doesn't support NULL bytes in VARCHAR)
    duck_conn.execute("SET pg_null_byte_replacement=''")
    
    duck_conn.execute("INSTALL postgres; LOAD postgres;")
    
    # Attach PostgreSQL as a DuckDB database
    dsn = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_pass}"
    duck_conn.execute(f"ATTACH '{dsn}' AS pg_db (TYPE POSTGRES)")
    
    # Special handling for estabelecimentos: create stub empresas for missing CNPJs
    if table_name == "estabelecimentos":
        logger.info("Ensuring all referenced empresas exist (creating stubs if needed)")
        
        # Collect all unique cnpj_basico from all parquet files using a temp table
        duck_conn.execute("CREATE TEMP TABLE missing_empresas (cnpj_basico VARCHAR)")
        
        for parquet_file in parquet_files:
            if not Path(parquet_file).exists():
                continue
                
            # Insert unique cnpj_basico values into temp table
            duck_conn.execute(f"""
                INSERT INTO missing_empresas
                SELECT DISTINCT cnpj_basico 
                FROM '{parquet_file}'
                WHERE cnpj_basico IS NOT NULL
            """)
        
        # Get count and create stubs in a single operation
        missing_count = duck_conn.execute("SELECT COUNT(DISTINCT cnpj_basico) FROM missing_empresas").fetchone()[0]
        
        if missing_count > 0:
            # Create stub empresas for CNPJs that don't exist
            duck_conn.execute(f"""
                INSERT INTO pg_db.{schema}.empresas (cnpj_basico, razao_social, reference_month)
                SELECT DISTINCT cnpj_basico, 'EMPRESA NAO CARREGADA', '{reference_month}'
                FROM missing_empresas
                ON CONFLICT (cnpj_basico) DO NOTHING
            """)
            
            logger.info(f"  Ensured {missing_count:,} empresas exist (stubs created if missing)")
        
        # Clean up temp table
        duck_conn.execute("DROP TABLE missing_empresas")
    
    try:
        for parquet_file in parquet_files:
            file_name = Path(parquet_file).name
            logger.info(f"  Loading {file_name}")
            file_start = time.time()
            
            # Verify file exists and is readable
            if not Path(parquet_file).exists():
                logger.warning(f"  File not found: {parquet_file}")
                continue
            
            # Get row count first (with retry for transient errors)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    row_count = duck_conn.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()[0]
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"  Retry {attempt+1}/{max_retries}: {e}")
                        time.sleep(1)
                    else:
                        raise
            
            if row_count == 0:
                logger.warning(f"  Empty file: {parquet_file}")
                continue
            
            # Get column names from parquet
            cols = duck_conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{parquet_file}')"
            ).fetchall()
            col_names = ', '.join(f'"{c[0]}"' for c in cols)
            
            # For large files, show progress by processing in batches
            if row_count > 1_000_000:
                batch_size = 500_000
                for offset in range(0, row_count, batch_size):
                    batch_start = time.time()
                    duck_conn.execute(f"""
                        INSERT INTO pg_db.{schema}.{table_name} ({col_names}, reference_month)
                        SELECT {col_names}, '{reference_month}'
                        FROM '{parquet_file}'
                        LIMIT {batch_size} OFFSET {offset}
                    """)
                    batch_duration = time.time() - batch_start
                    rows_done = min(offset + batch_size, row_count)
                    logger.info(f"    Progress: {rows_done:,}/{row_count:,} rows ({rows_done*100//row_count}%) - {batch_duration:.1f}s")
            else:
                duck_conn.execute(f"""
                    INSERT INTO pg_db.{schema}.{table_name} ({col_names}, reference_month)
                    SELECT {col_names}, '{reference_month}'
                    FROM '{parquet_file}'
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
        
    # Recreate heavy indexes after bulk load
    if heavy_indexes:
        logger.info(f"Recreating {len(heavy_indexes)} heavy indexes after bulk load")
        try:
            conn = psycopg2.connect(
                host=pg_host, port=pg_port, dbname=pg_db,
                user=pg_user, password=pg_pass
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                for idx_name, idx_def in heavy_indexes:
                    idx_start = time.time()
                    logger.info(f"  Creating {idx_name}...")
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{table_name} {idx_def}")
                    idx_duration = time.time() - idx_start
                    logger.info(f"    Created in {idx_duration:.1f}s")
            conn.close()
        except Exception as e:
            logger.error(f"Failed to recreate indexes: {e}")
            # Don't fail the task, just log error
    
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
    from neo4j import GraphDatabase
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to Neo4j as {entity_type}")
    start_time = time.time()
    
    # Neo4j connection
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    total_nodes = 0
    
    try:
        with driver.session() as session:
            for parquet_file in parquet_files:
                # TODO: Implement Neo4j loading logic
                # This is a placeholder for future implementation
                logger.info(f"  Neo4j loading for {Path(parquet_file).name} - Not yet implemented")
                pass
    
    except Exception as e:
        logger.error(f"Neo4j load failed: {e}")
        raise
    finally:
        driver.close()
    
    duration = time.time() - start_time
    throughput = total_nodes / duration if duration > 0 and total_nodes > 0 else 0
    
    logger.info(f"Loaded {total_nodes:,} nodes in {duration:.2f}s ({throughput:,.0f} nodes/sec)")
    
    return {
        "entity_type": entity_type,
        "files_loaded": len(parquet_files),
        "total_nodes": total_nodes,
        "duration_seconds": duration,
        "throughput_nodes_per_sec": throughput
    }
