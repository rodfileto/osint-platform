"""CNPJ graph step: load companies and partners from Parquet into Neo4j."""

import logging
import time
from pathlib import Path

import duckdb

from .config import PROCESSED_PATH

logger = logging.getLogger(__name__)


def load_entity(entity_type: str, reference_month: str) -> dict:
    """Load one month's companies ('Empresa') or partners ('Socio') into Neo4j."""
    pattern = "empresas_*.parquet" if entity_type == "Empresa" else "socios_*.parquet"
    files = sorted(str(p) for p in (PROCESSED_PATH / reference_month).glob(pattern))
    if not files:
        raise FileNotFoundError(f"No {pattern} for {reference_month} in {PROCESSED_PATH}")
    return load_to_neo4j(files, entity_type)


def load_to_neo4j(parquet_files: list[str], entity_type: str) -> dict:
    """
    Load Parquet files into Neo4j as nodes and relationships.
    
    Uses DuckDB to efficiently read parquet files in batches.
    Uses MERGE operations to handle both INSERT and UPDATE (idempotent).
    
    Args:
        parquet_files: List of Parquet file paths to load
        entity_type: Entity type (Empresa or Socio)
        
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
                # Pre-compute imports and schemas outside the batch loop
                import decimal
                from datetime import date as _date
                from .config import (
                    NEO4J_EMPRESA_FIELDS,
                    NEO4J_PESSOA_NODE_FIELDS,
                    NEO4J_SOCIO_REL_FIELDS,
                )

                def _neo4j_safe(v):
                    if isinstance(v, decimal.Decimal):
                        return float(v)
                    if isinstance(v, _date):
                        return v.isoformat()
                    return v

                # For Socio: derive reference_month from file path + build enriched schema
                if entity_type == "Socio":
                    import hashlib, re
                    month_match = re.search(r'(\d{4}-\d{2})', str(file_path))
                    ref_month = month_match.group(1) if month_match else "unknown"

                    # Schema with duplicate_count (computed once, reused per batch)
                    schema_with_dup = duck_conn.execute(f"""
                        DESCRIBE (
                            SELECT *,
                                COUNT(*) OVER (
                                    PARTITION BY cnpj_basico, cpf_cnpj_socio
                                ) AS duplicate_count
                            FROM '{parquet_file}'
                            LIMIT 1
                        )
                    """).fetchall()
                    cols_with_dup = [r[0] for r in schema_with_dup]

                for batch_start in range(0, total_file_rows, batch_size):
                    if entity_type == "Socio":
                        # Single enriched read — includes duplicate_count via window fn
                        raw_batch = duck_conn.execute(f"""
                            SELECT *,
                                COUNT(*) OVER (
                                    PARTITION BY cnpj_basico, cpf_cnpj_socio
                                ) AS duplicate_count
                            FROM '{parquet_file}'
                            LIMIT {batch_size} OFFSET {batch_start}
                        """).fetchall()

                        # Separate PJ socios from PF/Estrangeiro
                        pessoa_batch = []      # PF/Estrangeiro → Pessoa nodes
                        empresa_socio_batch = []  # PJ → Empresa-Empresa relationships
                        
                        for row in raw_batch:
                            rec = {k: _neo4j_safe(v) for k, v in zip(cols_with_dup, row)}

                            cpf_cnpj = rec.get('cpf_cnpj_socio') or ''
                            nome = rec.get('nome_socio_razao_social') or ''
                            id_socio = rec.get('identificador_socio', 0) or 0
                            
                            # PJ Sócio (identificador = 1) → Empresa-Empresa relationship
                            if id_socio == 1:
                                if not cpf_cnpj or len(cpf_cnpj) < 14:
                                    continue  # CNPJ inválido — descarta
                                
                                # Extrair CNPJ básico (primeiros 8 dígitos) do sócio PJ
                                cnpj_socio_basico = cpf_cnpj[:8].strip()
                                
                                empresa_socio_batch.append({
                                    'cnpj_socio_basico': cnpj_socio_basico,
                                    'cnpj_basico': rec['cnpj_basico'],
                                    'razao_social_socio': nome or None,
                                    'qualificacao_socio': rec.get('qualificacao_socio'),
                                    'data_entrada_sociedade': rec.get('data_entrada_sociedade'),
                                    'reference_month': ref_month,
                                    'duplicate_count': rec.get('duplicate_count', 1),
                                })
                            
                            # PF/Estrangeiro (identificador = 2, 3) → Pessoa nodes
                            else:
                                # Derivar pessoa_id para PF/Estrangeiro
                                if cpf_cnpj and nome:
                                    pessoa_id = hashlib.md5(f"{cpf_cnpj}|{nome}".encode()).hexdigest()
                                elif nome:
                                    pessoa_id = hashlib.md5(f"{nome}|{id_socio}".encode()).hexdigest()
                                else:
                                    continue  # descarta registro sem identidade mínima

                                pessoa_batch.append({
                                    # Nó :Pessoa — 5 props (modelo híbrido)
                                    'pessoa_id': pessoa_id,
                                    'nome': nome or None,
                                    'cpf_cnpj_socio': cpf_cnpj or None,
                                    'identificador_socio': id_socio,
                                    'faixa_etaria': rec.get('faixa_etaria'),
                                    # Empresa alvo do relacionamento
                                    'cnpj_basico': rec['cnpj_basico'],
                                    # Relacionamento [:SOCIO_DE] — 4 props (modelo híbrido)
                                    'qualificacao_socio': rec.get('qualificacao_socio'),
                                    'data_entrada_sociedade': rec.get('data_entrada_sociedade'),
                                    'reference_month': ref_month,
                                    'duplicate_count': rec.get('duplicate_count', 1),
                                })

                        # Load PF/Estrangeiro → Pessoa nodes + relationships
                        if pessoa_batch:
                            nodes_cr, rels_cr = session.execute_write(
                                _create_pessoa_socio_batch, pessoa_batch
                            )
                            total_nodes += nodes_cr
                            total_relationships += rels_cr
                        
                        # Load PJ → Empresa-Empresa relationships
                        if empresa_socio_batch:
                            rels_cr = session.execute_write(
                                _create_empresa_socio_batch, empresa_socio_batch
                            )
                            total_relationships += rels_cr

                        batch_end = min(batch_start + batch_size, total_file_rows)
                        if batch_end < total_file_rows:
                            logger.info(f"    Progress: {batch_end:,}/{total_file_rows:,} rows ({batch_end*100//total_file_rows}%)")
                        continue  # pula o bloco Empresa

                    # ---- Empresa path ----
                    # Read batch directly from parquet using DuckDB
                    batch_records = duck_conn.execute(f"""
                        SELECT * FROM '{parquet_file}'
                        LIMIT {batch_size} OFFSET {batch_start}
                    """).fetchall()

                    allowed = NEO4J_EMPRESA_FIELDS

                    batch_dicts = [
                        {k: _neo4j_safe(v) for k, v in zip(columns, row) if k in allowed}
                        for row in batch_records
                    ]
                    
                    total_nodes += session.execute_write(_create_empresa_nodes, batch_dicts)
                    
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
    Remove _stub flag if present (stubs criados pela carga de sócios são enriquecidos aqui).

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


def _create_pessoa_socio_batch(tx, records):
    """
    Transaction function for the Modelo Híbrido de Sócios - PF/Estrangeiro only.

    Cria/atualiza nós :Pessoa (5 props) e relacionamentos [:SOCIO_DE] (4 props)
    apontando para :Empresa (MERGE stub).

    Estrutura do nó :Pessoa (modelo híbrido):
        pessoa_id           — chave de deduplicação (MD5 de cpf_mask|nome)
        nome                — nome_socio_razao_social
        cpf_cnpj_socio      — CPF mascarado pela RF
        identificador_socio — 2=PF, 3=Estrangeiro
        faixa_etaria        — código de faixa etária (alta utilidade OSINT)

    Estrutura do relacionamento [:SOCIO_DE] (modelo híbrido):
        qualificacao_socio       — tipo de vínculo societário
        data_entrada_sociedade   — data de ingresso na empresa
        reference_month          — snapshot mensal (chave de MERGE do rel)
        duplicate_count          — nº de ocorrências do par pessoa×empresa no snapshot

    Nota de design:
        - O MERGE do relacionamento usa {reference_month} como chave discriminante,
          permitindo múltiplos snapshots mensais entre o mesmo par (Pessoa, Empresa).
        - A Empresa é criada como stub se não existir — será enriquecida pela carga
          de empresas.parquet.
    """
    query = """
    UNWIND $records AS record
    MERGE (p:Pessoa {pessoa_id: record.pessoa_id})
    ON CREATE SET p.created_at = datetime()
    SET p.nome                = record.nome,
        p.cpf_cnpj_socio      = record.cpf_cnpj_socio,
        p.identificador_socio = record.identificador_socio,
        p.faixa_etaria        = record.faixa_etaria,
        p.updated_at          = datetime()
    WITH p, record
    MERGE (e:Empresa {cnpj_basico: record.cnpj_basico})
    ON CREATE SET e.created_at = datetime(), e._stub = true
    MERGE (p)-[r:SOCIO_DE {reference_month: record.reference_month}]->(e)
    ON CREATE SET r.created_at = datetime()
    SET r.qualificacao_socio      = record.qualificacao_socio,
        r.data_entrada_sociedade  = record.data_entrada_sociedade,
        r.duplicate_count         = record.duplicate_count,
        r.updated_at              = datetime()
    RETURN COUNT(DISTINCT p) AS nodes_created, COUNT(DISTINCT r) AS relationships_created
    """
    result = tx.run(query, records=records)
    row = result.single()
    return row["nodes_created"], row["relationships_created"]


def _create_empresa_socio_batch(tx, records):
    """
    Transaction function for PJ Socios - Empresa→Empresa relationships.

    Cria relacionamentos [:SOCIO_DE] entre duas empresas quando uma empresa 
    é sócia de outra (identificador_socio = 1).

    Estrutura do relacionamento [:SOCIO_DE] para PJ:
        qualificacao_socio       — tipo de vínculo societário
        data_entrada_sociedade   — data de ingresso na empresa
        reference_month          — snapshot mensal (chave de MERGE do rel)
        duplicate_count          — nº de ocorrências do par empresa×empresa no snapshot
        razao_social_socio       — razão social da empresa sócia (opcional, para enriquecimento)

    Nota de design:
        - Ambas as Empresas (sócia e alvo) são criadas como stubs se não existirem,
          para serem enriquecidas posteriormente pela carga de empresas.parquet.
        - O MERGE do relacionamento usa {reference_month} como chave discriminante,
          permitindo múltiplos snapshots mensais entre o mesmo par de empresas.
        - Não cria nó :Pessoa — relacionamento direto Empresa→Empresa.
    """
    query = """
    UNWIND $records AS record
    MERGE (socio:Empresa {cnpj_basico: record.cnpj_socio_basico})
    ON CREATE SET socio.created_at = datetime(), socio._stub = true
    WITH socio, record
    MERGE (alvo:Empresa {cnpj_basico: record.cnpj_basico})
    ON CREATE SET alvo.created_at = datetime(), alvo._stub = true
    MERGE (socio)-[r:SOCIO_DE {reference_month: record.reference_month}]->(alvo)
    ON CREATE SET r.created_at = datetime()
    SET r.qualificacao_socio       = record.qualificacao_socio,
        r.data_entrada_sociedade   = record.data_entrada_sociedade,
        r.duplicate_count          = record.duplicate_count,
        r.razao_social_socio       = record.razao_social_socio,
        r.updated_at               = datetime()
    RETURN COUNT(DISTINCT r) AS relationships_created
    """
    result = tx.run(query, records=records)
    return result.single()["relationships_created"]