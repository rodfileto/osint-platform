"""
CNPJ Neo4j Sync DAG

Simple, focused DAG to sync CNPJ data from PostgreSQL to Neo4j.
This is a standalone sync tool - does not interfere with the main ingestion DAG.

Purpose:
- Load Empresas and Estabelecimentos from PostgreSQL to Neo4j
- Create relationships (PERTENCE_A)
- Use batched processing for memory efficiency
"""

from datetime import datetime, timedelta
import logging
import os
import time
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

@task
def clear_neo4j_cnpj_data() -> dict:
    """Clear existing CNPJ data from Neo4j before reload."""
    from neo4j import GraphDatabase
    
    logger.info("Clearing existing CNPJ data from Neo4j")
    start_time = time.time()
    
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    try:
        with driver.session() as session:
            # Delete all Estabelecimento and Empresa nodes
            result = session.run("""
                MATCH (n)
                WHERE n:Empresa OR n:Estabelecimento
                DETACH DELETE n
                RETURN count(n) as deleted
            """)
            deleted = result.single()["deleted"]
            
        duration = time.time() - start_time
        logger.info(f"Cleared {deleted:,} nodes in {duration:.2f}s")
        
        return {"deleted_nodes": deleted, "duration_seconds": duration}
        
    finally:
        driver.close()


@task
def sync_empresas_to_neo4j(batch_size: int = 50000) -> dict:
    """
    Sync Empresa nodes from PostgreSQL to Neo4j.
    Uses batched UNWIND for optimal performance.
    """
    import psycopg2
    from neo4j import GraphDatabase
    
    logger.info("Syncing Empresas from PostgreSQL to Neo4j")
    start_time = time.time()
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password")
    )
    
    # Connect to Neo4j
    neo4j_driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    total_nodes = 0
    batch_count = 0
    
    try:
        # Get total count first (using index)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cnpj.empresa")
            total_count = cur.fetchone()[0]
            logger.info(f"Total empresas to sync: {total_count:,}")
        
        # Stream data in batches using server-side cursor
        with pg_conn.cursor(name='empresa_cursor') as cur:
            cur.itersize = batch_size
            cur.execute("""
                SELECT 
                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    capital_social,
                    porte_empresa,
                    ente_federativo_responsavel,
                    reference_month
                FROM cnpj.empresa
                ORDER BY cnpj_basico
            """)
            
            with neo4j_driver.session() as neo4j_session:
                batch = []
                
                for row in cur:
                    batch.append({
                        "cnpj_basico": row[0],
                        "razao_social": row[1],
                        "natureza_juridica": row[2],
                        "qualificacao_responsavel": row[3],
                        "capital_social": float(row[4]) if row[4] else None,
                        "porte_empresa": row[5],
                        "ente_federativo": row[6],
                        "reference_month": row[7]
                    })
                    
                    if len(batch) >= batch_size:
                        # Load batch to Neo4j using UNWIND
                        neo4j_session.run("""
                            UNWIND $batch AS empresa
                            MERGE (e:Empresa {cnpj_basico: empresa.cnpj_basico})
                            SET e.razao_social = empresa.razao_social,
                                e.natureza_juridica = empresa.natureza_juridica,
                                e.qualificacao_responsavel = empresa.qualificacao_responsavel,
                                e.capital_social = empresa.capital_social,
                                e.porte_empresa = empresa.porte_empresa,
                                e.ente_federativo = empresa.ente_federativo,
                                e.reference_month = empresa.reference_month,
                                e.updated_at = datetime()
                        """, batch=batch)
                        
                        total_nodes += len(batch)
                        batch_count += 1
                        
                        # Log progress
                        progress_pct = (total_nodes / total_count * 100) if total_count > 0 else 0
                        elapsed = time.time() - start_time
                        rate = total_nodes / elapsed if elapsed > 0 else 0
                        eta = (total_count - total_nodes) / rate if rate > 0 else 0
                        
                        logger.info(
                            f"  Batch {batch_count}: {total_nodes:,}/{total_count:,} ({progress_pct:.1f}%) "
                            f"- {rate:,.0f} nodes/sec - ETA: {eta/60:.1f}m"
                        )
                        
                        batch = []
                
                # Load remaining batch
                if batch:
                    neo4j_session.run("""
                        UNWIND $batch AS empresa
                        MERGE (e:Empresa {cnpj_basico: empresa.cnpj_basico})
                        SET e.razao_social = empresa.razao_social,
                            e.natureza_juridica = empresa.natureza_juridica,
                            e.qualificacao_responsavel = empresa.qualificacao_responsavel,
                            e.capital_social = empresa.capital_social,
                            e.porte_empresa = empresa.porte_empresa,
                            e.ente_federativo = empresa.ente_federativo,
                            e.reference_month = empresa.reference_month,
                            e.updated_at = datetime()
                    """, batch=batch)
                    total_nodes += len(batch)
                    batch_count += 1
        
        duration = time.time() - start_time
        throughput = total_nodes / duration if duration > 0 else 0
        
        logger.info(f"Synced {total_nodes:,} Empresa nodes in {duration:.2f}s ({throughput:,.0f} nodes/sec)")
        
        return {
            "entity_type": "Empresa",
            "total_nodes": total_nodes,
            "batches": batch_count,
            "duration_seconds": duration,
            "throughput_nodes_per_sec": throughput
        }
        
    finally:
        pg_conn.close()
        neo4j_driver.close()


@task
def create_empresa_index() -> dict:
    """Create index on Empresa.cnpj_basico for fast lookups."""
    from neo4j import GraphDatabase
    
    logger.info("Creating index on Empresa.cnpj_basico")
    start_time = time.time()
    
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    try:
        with driver.session() as session:
            # Create constraint (automatically creates index)
            session.run("""
                CREATE CONSTRAINT empresa_cnpj_basico IF NOT EXISTS
                FOR (e:Empresa) REQUIRE e.cnpj_basico IS UNIQUE
            """)
            
        duration = time.time() - start_time
        logger.info(f"Index created in {duration:.2f}s")
        
        return {"duration_seconds": duration}
        
    finally:
        driver.close()


@task
def sync_estabelecimentos_to_neo4j(batch_size: int = 50000) -> dict:
    """
    Sync Estabelecimento nodes from PostgreSQL to Neo4j.
    Creates nodes and PERTENCE_A relationships to Empresa.
    """
    import psycopg2
    from neo4j import GraphDatabase
    
    logger.info("Syncing Estabelecimentos from PostgreSQL to Neo4j")
    start_time = time.time()
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password")
    )
    
    # Connect to Neo4j
    neo4j_driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "osint_graph_password"))
    )
    
    total_nodes = 0
    total_relationships = 0
    batch_count = 0
    
    try:
        # Get total count
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cnpj.estabelecimento")
            total_count = cur.fetchone()[0]
            logger.info(f"Total estabelecimentos to sync: {total_count:,}")
        
        # Stream data in batches
        with pg_conn.cursor(name='estabelecimento_cursor') as cur:
            cur.itersize = batch_size
            cur.execute("""
                SELECT 
                    cnpj_basico,
                    cnpj_ordem,
                    cnpj_dv,
                    identificador_matriz_filial,
                    nome_fantasia,
                    situacao_cadastral,
                    data_situacao_cadastral,
                    motivo_situacao_cadastral,
                    nome_cidade_exterior,
                    pais,
                    data_inicio_atividade,
                    cnae_fiscal_principal,
                    cnae_fiscal_secundaria,
                    tipo_logradouro,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cep,
                    uf,
                    municipio,
                    ddd_1,
                    telefone_1,
                    ddd_2,
                    telefone_2,
                    ddd_fax,
                    fax,
                    email,
                    situacao_especial,
                    data_situacao_especial,
                    reference_month
                FROM cnpj.estabelecimento
                ORDER BY cnpj_basico, cnpj_ordem
            """)
            
            with neo4j_driver.session() as neo4j_session:
                batch = []
                
                for row in cur:
                    cnpj_completo = f"{row[0]}{row[1]}{row[2]}" if row[0] and row[1] and row[2] else None
                    
                    batch.append({
                        "cnpj_basico": row[0],
                        "cnpj_ordem": row[1],
                        "cnpj_dv": row[2],
                        "cnpj_completo": cnpj_completo,
                        "identificador_matriz_filial": row[3],
                        "nome_fantasia": row[4],
                        "situacao_cadastral": row[5],
                        "data_situacao_cadastral": str(row[6]) if row[6] else None,
                        "motivo_situacao_cadastral": row[7],
                        "nome_cidade_exterior": row[8],
                        "pais": row[9],
                        "data_inicio_atividade": str(row[10]) if row[10] else None,
                        "cnae_fiscal_principal": row[11],
                        "cnae_fiscal_secundaria": row[12],
                        "tipo_logradouro": row[13],
                        "logradouro": row[14],
                        "numero": row[15],
                        "complemento": row[16],
                        "bairro": row[17],
                        "cep": row[18],
                        "uf": row[19],
                        "municipio": row[20],
                        "ddd_1": row[21],
                        "telefone_1": row[22],
                        "ddd_2": row[23],
                        "telefone_2": row[24],
                        "ddd_fax": row[25],
                        "fax": row[26],
                        "email": row[27],
                        "situacao_especial": row[28],
                        "data_situacao_especial": str(row[29]) if row[29] else None,
                        "reference_month": row[30]
                    })
                    
                    if len(batch) >= batch_size:
                        # Load batch with relationship - MERGE for idempotent updates
                        result = neo4j_session.run("""
                            UNWIND $batch AS est
                            MERGE (e:Estabelecimento {cnpj_completo: est.cnpj_completo})
                            SET e.cnpj_basico = est.cnpj_basico,
                                e.cnpj_ordem = est.cnpj_ordem,
                                e.cnpj_dv = est.cnpj_dv,
                                e.identificador_matriz_filial = est.identificador_matriz_filial,
                                e.nome_fantasia = est.nome_fantasia,
                                e.situacao_cadastral = est.situacao_cadastral,
                                e.data_situacao_cadastral = est.data_situacao_cadastral,
                                e.motivo_situacao_cadastral = est.motivo_situacao_cadastral,
                                e.nome_cidade_exterior = est.nome_cidade_exterior,
                                e.pais = est.pais,
                                e.data_inicio_atividade = est.data_inicio_atividade,
                                e.cnae_fiscal_principal = est.cnae_fiscal_principal,
                                e.cnae_fiscal_secundaria = est.cnae_fiscal_secundaria,
                                e.tipo_logradouro = est.tipo_logradouro,
                                e.logradouro = est.logradouro,
                                e.numero = est.numero,
                                e.complemento = est.complemento,
                                e.bairro = est.bairro,
                                e.cep = est.cep,
                                e.uf = est.uf,
                                e.municipio = est.municipio,
                                e.ddd_1 = est.ddd_1,
                                e.telefone_1 = est.telefone_1,
                                e.ddd_2 = est.ddd_2,
                                e.telefone_2 = est.telefone_2,
                                e.ddd_fax = est.ddd_fax,
                                e.fax = est.fax,
                                e.email = est.email,
                                e.situacao_especial = est.situacao_especial,
                                e.data_situacao_especial = est.data_situacao_especial,
                                e.reference_month = est.reference_month,
                                e.updated_at = datetime()
                            WITH e, est
                            MATCH (emp:Empresa {cnpj_basico: est.cnpj_basico})
                            MERGE (e)-[r:PERTENCE_A]->(emp)
                            RETURN count(e) as nodes_created, count(r) as rels_created
                        """, batch=batch)
                        
                        summary = result.single()
                        total_nodes += summary["nodes_created"]
                        total_relationships += summary["rels_created"]
                        batch_count += 1
                        
                        # Log progress
                        progress_pct = (total_nodes / total_count * 100) if total_count > 0 else 0
                        elapsed = time.time() - start_time
                        rate = total_nodes / elapsed if elapsed > 0 else 0
                        eta = (total_count - total_nodes) / rate if rate > 0 else 0
                        
                        logger.info(
                            f"  Batch {batch_count}: {total_nodes:,}/{total_count:,} ({progress_pct:.1f}%) "
                            f"- {rate:,.0f} nodes/sec - ETA: {eta/60:.1f}m"
                        )
                        
                        batch = []
                
                # Load remaining batch
                if batch:
                    result = neo4j_session.run("""
                        UNWIND $batch AS est
                        MERGE (e:Estabelecimento {cnpj_completo: est.cnpj_completo})
                        SET e.cnpj_basico = est.cnpj_basico,
                            e.cnpj_ordem = est.cnpj_ordem,
                            e.cnpj_dv = est.cnpj_dv,
                            e.identificador_matriz_filial = est.identificador_matriz_filial,
                            e.nome_fantasia = est.nome_fantasia,
                            e.situacao_cadastral = est.situacao_cadastral,
                            e.data_situacao_cadastral = est.data_situacao_cadastral,
                            e.motivo_situacao_cadastral = est.motivo_situacao_cadastral,
                            e.nome_cidade_exterior = est.nome_cidade_exterior,
                            e.pais = est.pais,
                            e.data_inicio_atividade = est.data_inicio_atividade,
                            e.cnae_fiscal_principal = est.cnae_fiscal_principal,
                            e.cnae_fiscal_secundaria = est.cnae_fiscal_secundaria,
                            e.tipo_logradouro = est.tipo_logradouro,
                            e.logradouro = est.logradouro,
                            e.numero = est.numero,
                            e.complemento = est.complemento,
                            e.bairro = est.bairro,
                            e.cep = est.cep,
                            e.uf = est.uf,
                            e.municipio = est.municipio,
                            e.ddd_1 = est.ddd_1,
                            e.telefone_1 = est.telefone_1,
                            e.ddd_2 = est.ddd_2,
                            e.telefone_2 = est.telefone_2,
                            e.ddd_fax = est.ddd_fax,
                            e.fax = est.fax,
                            e.email = est.email,
                            e.situacao_especial = est.situacao_especial,
                            e.data_situacao_especial = est.data_situacao_especial,
                            e.reference_month = est.reference_month,
                            e.updated_at = datetime()
                        WITH e, est
                        MATCH (emp:Empresa {cnpj_basico: est.cnpj_basico})
                        MERGE (e)-[r:PERTENCE_A]->(emp)
                        RETURN count(e) as nodes_created, count(r) as rels_created
                    """, batch=batch)
                    
                    summary = result.single()
                    total_nodes += summary["nodes_created"]
                    total_relationships += summary["rels_created"]
                    batch_count += 1
        
        duration = time.time() - start_time
        throughput = total_nodes / duration if duration > 0 else 0
        
        logger.info(
            f"Synced {total_nodes:,} Estabelecimento nodes and {total_relationships:,} relationships "
            f"in {duration:.2f}s ({throughput:,.0f} nodes/sec)"
        )
        
        return {
            "entity_type": "Estabelecimento",
            "total_nodes": total_nodes,
            "total_relationships": total_relationships,
            "batches": batch_count,
            "duration_seconds": duration,
            "throughput_nodes_per_sec": throughput
        }
        
    finally:
        pg_conn.close()
        neo4j_driver.close()


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),
}

with DAG(
    dag_id='cnpj_sync_neo4j',
    default_args=default_args,
    description='Sync CNPJ data from PostgreSQL to Neo4j',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'neo4j', 'sync'],
    params={
        'batch_size': 50000,
        'clear_before_sync': False,  # Set to True for full reload, False for incremental
        'sync_empresas': True,
        'sync_estabelecimentos': True,
    },
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Conditional clear (only if clear_before_sync=True)
    # For incremental updates, skip this step
    clear = clear_neo4j_cnpj_data()
    
    # Sync empresas (uses MERGE - idempotent)
    sync_empresas = sync_empresas_to_neo4j(batch_size=50000)
    
    # Create index for fast lookups
    create_idx = create_empresa_index()
    
    # Sync estabelecimentos (now uses MERGE - idempotent, can run multiple times)
    sync_estabelecimentos = sync_estabelecimentos_to_neo4j(batch_size=50000)
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies based on use case:
    # FULL RELOAD (first time or after schema changes):
    #   start >> clear >> sync_empresas >> create_idx >> sync_estabelecimentos >> end
    #
    # INCREMENTAL UPDATE (just estabelecimentos from new month):
    #   Comment out 'clear' task or manually skip it in Airflow UI
    #   Uses MERGE so it's safe to re-run - updates existing, creates new
    #
    # For now: full reload path (includes clear)
    start >> clear >> sync_empresas >> create_idx >> sync_estabelecimentos >> end


if __name__ == "__main__":
    dag.test()
