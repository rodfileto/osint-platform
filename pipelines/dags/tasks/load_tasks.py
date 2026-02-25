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
def load_to_postgresql(parquet_files: list[str], table_name: str, schema: str = "cnpj", reference_month: str = "2024-02", **context) -> dict:
    """
    Load Parquet files into PostgreSQL using DuckDB's postgres extension.
    
    Uses DuckDB to read parquet and INSERT directly into PostgreSQL
    via the native postgres scanner — no Python row iteration needed.
    
    Supports UPSERT mode when force_reprocess=True in DAG params.
    
    Args:
        parquet_files: List of Parquet file paths to load
        table_name: Target table name (empresas, estabelecimentos)
        schema: PostgreSQL schema name
        reference_month: Reference month for the data (YYYY-MM format)
        context: Airflow context (includes DAG params)
        
    Returns:
        Dict with load stats
    """
    force_upsert = context.get('params', {}).get('force_reprocess', False)
    
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "osint_metadata")
    pg_user = os.getenv("POSTGRES_USER", "osint_admin")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "osint_secure_password")
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to PostgreSQL {schema}.{table_name} (UPSERT={force_upsert})")
    start_time = time.time()
    
    # Identify heavy indexes to drop/recreate for bulk load performance
    heavy_indexes = []
    if table_name == "empresa":
        heavy_indexes = [("idx_empresas_razao_social", "USING GIN (to_tsvector('portuguese', razao_social))")]
    elif table_name == "estabelecimento":
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
                
            # If UPSERT mode, create staging table with same structure
            if force_upsert:
                logger.info(f"Creating staging table {schema}.{table_name}_staging")
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}_staging CASCADE")
                cur.execute(f"""
                    CREATE TABLE {schema}.{table_name}_staging 
                    (LIKE {schema}.{table_name} INCLUDING DEFAULTS)
                """)
                
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
    
    # Special handling para estabelecimento: garante que toda empresa referenciada existe
    # (cnpj_basico pode estar em reference_month diferente — cria stub para não violar FK)
    if table_name == "estabelecimento":
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
        
        # Conta apenas os cnpj_basico realmente ausentes na tabela empresa
        missing_count = duck_conn.execute(f"""
            SELECT COUNT(DISTINCT m.cnpj_basico)
            FROM missing_empresas m
            LEFT JOIN pg_db.{schema}.empresa e ON e.cnpj_basico = m.cnpj_basico
            WHERE e.cnpj_basico IS NULL
        """).fetchone()[0]
        
        if missing_count > 0:
            logger.info(f"  Creating {missing_count:,} stub empresa records for orphan cnpj_basico")
            # Cria stub apenas para os cnpj_basico ausentes
            duck_conn.execute(f"""
                INSERT INTO pg_db.{schema}.empresa (cnpj_basico, razao_social, reference_month)
                SELECT DISTINCT m.cnpj_basico, 'EMPRESA NAO CARREGADA', '{reference_month}'
                FROM missing_empresas m
                LEFT JOIN pg_db.{schema}.empresa e ON e.cnpj_basico = m.cnpj_basico
                WHERE e.cnpj_basico IS NULL
                ON CONFLICT (cnpj_basico) DO NOTHING
            """)
            logger.info(f"  Stubs created OK")
        else:
            logger.info("  All referenced empresas already exist — no stubs needed")
        
        # Clean up temp table
        duck_conn.execute("DROP TABLE missing_empresas")
    
    # Verifica uma vez se reference_month já foi carregado (evita duplicate key em re-runs)
    if not force_upsert:
        already_loaded = duck_conn.execute(f"""
            SELECT COUNT(*) FROM pg_db.{schema}.{table_name}
            WHERE reference_month = '{reference_month}'
            LIMIT 1
        """).fetchone()[0]
        if already_loaded > 0:
            logger.info(
                f"Skipping {table_name} — reference_month {reference_month} já carregado "
                f"({already_loaded:,} rows). Use force_reprocess=True para recarregar."
            )
            duck_conn.execute("DETACH pg_db")
            duck_conn.close()
            return {"table": table_name, "reference_month": reference_month, "row_count": 0, "skipped": True}

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
            
            # Determine target table (staging if UPSERT, final if INSERT)
            target_table = f"{schema}.{table_name}_staging" if force_upsert else f"{schema}.{table_name}"
            target_db_ref = f"pg_db.{target_table}"
            
            # For large files, show progress by processing in batches
            if row_count > 1_000_000:
                batch_size = 500_000
                for offset in range(0, row_count, batch_size):
                    batch_start = time.time()
                    duck_conn.execute(f"""
                        INSERT INTO {target_db_ref} ({col_names}, reference_month)
                        SELECT {col_names}, '{reference_month}'
                        FROM '{parquet_file}'
                        LIMIT {batch_size} OFFSET {offset}
                    """)
                    batch_duration = time.time() - batch_start
                    rows_done = min(offset + batch_size, row_count)
                    logger.info(f"    Progress: {rows_done:,}/{row_count:,} rows ({rows_done*100//row_count}%) - {batch_duration:.1f}s")
            else:
                duck_conn.execute(f"""
                    INSERT INTO {target_db_ref} ({col_names}, reference_month)
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
    
    # If UPSERT mode, execute INSERT ... ON CONFLICT from staging to final table
    if force_upsert and total_rows > 0:
        logger.info(f"Executing UPSERT from staging table to {schema}.{table_name}")
        upsert_start = time.time()
        
        try:
            conn = psycopg2.connect(
                host=pg_host, port=pg_port, dbname=pg_db,
                user=pg_user, password=pg_pass
            )
            conn.autocommit = False
            
            with conn.cursor() as cur:
                # Get all column names except created_at (preserve original)
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                        AND table_name = '{table_name}'
                        AND column_name NOT IN ('created_at')
                    ORDER BY ordinal_position
                """)
                all_cols = [row[0] for row in cur.fetchall()]
                col_list = ', '.join(all_cols)
                
                # Check if updated_at column exists
                has_updated_at = 'updated_at' in all_cols
                
                # Build UPDATE SET clause (update all columns)
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in all_cols])
                
                # Add updated_at if it exists and wasn't already in the list
                if has_updated_at and 'updated_at' not in update_set:
                    update_set += ", updated_at = CURRENT_TIMESTAMP"
                elif has_updated_at:
                    # Replace the EXCLUDED.updated_at with CURRENT_TIMESTAMP
                    update_set = update_set.replace("updated_at = EXCLUDED.updated_at", "updated_at = CURRENT_TIMESTAMP")
                
                # Determine primary key constraint for ON CONFLICT clause
                if table_name == "empresa":
                    conflict_clause = "cnpj_basico"
                elif table_name == "estabelecimento":
                    conflict_clause = "cnpj_basico, cnpj_ordem, cnpj_dv"
                else:
                    raise ValueError(f"Unknown table for UPSERT: {table_name}")
                
                # Execute UPSERT
                upsert_sql = f"""
                    INSERT INTO {schema}.{table_name} ({col_list})
                    SELECT {col_list}
                    FROM {schema}.{table_name}_staging
                    ON CONFLICT ({conflict_clause}) DO UPDATE SET
                        {update_set}
                """
                
                logger.info(f"  Executing UPSERT with ON CONFLICT ({conflict_clause})")
                cur.execute(upsert_sql)
                upserted_count = cur.rowcount
                conn.commit()
                
                upsert_duration = time.time() - upsert_start
                logger.info(f"  UPSERT completed: {upserted_count:,} rows in {upsert_duration:.1f}s")
                
                # Drop staging table
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}_staging")
                conn.commit()
                
        except Exception as e:
            logger.error(f"UPSERT failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
        
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
def load_to_neo4j(parquet_files: list[str], entity_type: str, **context) -> dict:
    """
    Load Parquet files into Neo4j as nodes and relationships.
    
    Uses DuckDB to efficiently read parquet files in batches.
    Uses MERGE operations to handle both INSERT and UPDATE (idempotent).
    
    Args:
        parquet_files: List of Parquet file paths to load
        entity_type: Entity type (Empresa, Estabelecimento)
        context: Airflow context (includes DAG params)
        
    Returns:
        Dict with load stats
    """
    from neo4j import GraphDatabase
    from .config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    
    logger.info(f"Loading {len(parquet_files)} Parquet files to Neo4j as {entity_type}")
    start_time = time.time()
    
    # Neo4j connection
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    # DuckDB connection for reading parquet
    duck_conn = duckdb.connect(':memory:')
    
    total_nodes = 0
    total_relationships = 0
    batch_size = 50000  # 50k rows per transaction — balances memory and throughput for 66M+ records
    
    try:
        with driver.session() as session:
            for parquet_file in parquet_files:
                file_path = Path(parquet_file)
                if not file_path.exists():
                    logger.warning(f"  File not found: {parquet_file}")
                    continue
                
                logger.info(f"  Processing {file_path.name}")
                file_start = time.time()
                
                # Get row count and column names from parquet schema
                schema_rows = duck_conn.execute(
                    f"DESCRIBE SELECT * FROM '{parquet_file}'"
                ).fetchall()
                columns = [row[0] for row in schema_rows]

                total_file_rows = duck_conn.execute(
                    f"SELECT COUNT(*) FROM '{parquet_file}'"
                ).fetchone()[0]
                
                if total_file_rows == 0:
                    logger.warning(f"    Empty file: {file_path.name}")
                    continue
                
                # Process in batches using DuckDB's LIMIT/OFFSET
                for batch_start in range(0, total_file_rows, batch_size):
                    # Read batch directly from parquet using DuckDB
                    batch_records = duck_conn.execute(f"""
                        SELECT * FROM '{parquet_file}'
                        LIMIT {batch_size} OFFSET {batch_start}
                    """).fetchall()
                    
                    # Filtra apenas os campos permitidos para o Neo4j e converte
                    # tipos não suportados pelo driver (ex: Decimal → float).
                    # Campos analíticos (capital_social, endereço, contatos) ficam
                    # exclusivamente no PostgreSQL.
                    import decimal
                    from .config import NEO4J_EMPRESA_FIELDS, NEO4J_ESTABELECIMENTO_FIELDS
                    allowed = NEO4J_EMPRESA_FIELDS if entity_type == "Empresa" else NEO4J_ESTABELECIMENTO_FIELDS

                    def _neo4j_safe(v):
                        if isinstance(v, decimal.Decimal):
                            return float(v)
                        return v

                    batch_dicts = [
                        {k: _neo4j_safe(v) for k, v in zip(columns, row) if k in allowed}
                        for row in batch_records
                    ]
                    
                    if entity_type == "Empresa":
                        # Create/update Empresa nodes
                        result = session.execute_write(
                            _create_empresa_nodes, batch_dicts
                        )
                        total_nodes += result
                        
                    elif entity_type == "Estabelecimento":
                        # Create/update Estabelecimento nodes and relationships
                        nodes_created, rels_created = session.execute_write(
                            _create_estabelecimento_nodes, batch_dicts
                        )
                        total_nodes += nodes_created
                        total_relationships += rels_created
                    
                    batch_end = min(batch_start + batch_size, total_file_rows)
                    if batch_end < total_file_rows:
                        logger.info(f"    Progress: {batch_end:,}/{total_file_rows:,} rows ({batch_end*100//total_file_rows}%)")
                
                file_duration = time.time() - file_start
                logger.info(f"    Loaded {total_file_rows:,} records in {file_duration:.1f}s")
        
    except Exception as e:
        logger.error(f"Neo4j load failed: {e}")
        raise
    finally:
        duck_conn.close()
        driver.close()
    
    duration = time.time() - start_time
    throughput = total_nodes / duration if duration > 0 and total_nodes > 0 else 0
    
    logger.info(
        f"Loaded {total_nodes:,} nodes and {total_relationships:,} relationships "
        f"in {duration:.2f}s ({throughput:,.0f} nodes/sec)"
    )
    
    return {
        "entity_type": entity_type,
        "files_loaded": len(parquet_files),
        "total_nodes": total_nodes,
        "total_relationships": total_relationships,
        "duration_seconds": duration,
        "throughput_nodes_per_sec": throughput
    }


def _create_empresa_nodes(tx, records):
    """
    Transaction function to create/update Empresa nodes.
    Uses MERGE to handle both insert and update.
    Remove _stub flag if present (stubs criados por estabelecimentos são enriquecidos aqui).

    Ordem Cypher correta: ON CREATE SET antes do SET livre.
    """
    query = """
    UNWIND $records AS record
    MERGE (e:Empresa {cnpj_basico: record.cnpj_basico})
    ON CREATE SET e.created_at = datetime()
    SET e += record,
        e.updated_at = datetime()
    REMOVE e._stub
    RETURN COUNT(e) AS nodes_created
    """
    result = tx.run(query, records=records)
    return result.single()["nodes_created"]


def _create_estabelecimento_nodes(tx, records):
    """
    Transaction function to create/update Estabelecimento nodes and relationships.
    Single atomic transaction: node + MERGE Empresa stub + relationship.

    Empresa MERGE só cria stub (ON CREATE SET) — nunca sobrescreve com dados
    do estabelecimento. Stub é enriquecido quando empresas forem carregadas.
    Ordem Cypher correta: ON CREATE SET antes do SET livre.
    """
    # Pre-compute CNPJ 14 dígitos no Python (mais rápido que concatenar no Cypher)
    for record in records:
        record['cnpj'] = f"{record['cnpj_basico']}{record['cnpj_ordem']}{record['cnpj_dv']}"

    # Tudo em uma única transação: nó Estabelecimento + stub Empresa + relacionamento
    query = """
    UNWIND $records AS record
    MERGE (est:Estabelecimento {cnpj: record.cnpj})
    ON CREATE SET est.created_at = datetime()
    SET est += record,
        est.updated_at = datetime()
    WITH est, record
    MERGE (e:Empresa {cnpj_basico: record.cnpj_basico})
    ON CREATE SET e.created_at = datetime(), e._stub = true
    MERGE (est)-[r:PERTENCE_A]->(e)
    ON CREATE SET r.created_at = datetime()
    RETURN COUNT(DISTINCT est) AS nodes_created, COUNT(DISTINCT r) AS relationships_created
    """
    result = tx.run(query, records=records)
    row = result.single()
    return row["nodes_created"], row["relationships_created"]
