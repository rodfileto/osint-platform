"""
INPI Patents Neo4j Loader

Lê os dados de patentes do PostgreSQL (schema inpi) e popula o grafo Neo4j.

Nós criados:
  :Patente         — um nó por patente (chave: codigo_interno)
  :ClasseIPC       — símbolo IPC único (chave: simbolo)
  :DepositantePF   — pessoa física depositante (chave: SHA-256 de nome+cpf_mascara)

Relacionamentos criados:
  (Empresa)-[:DEPOSITOU]->(Patente)         via cnpj_basico_resolved IS NOT NULL
  (DepositantePF)-[:DEPOSITOU]->(Patente)   via tipopessoadepositante='pessoa física'
  (Patente)-[:CLASSIFICADA_COMO]->(ClasseIPC)
  (Patente)-[:VINCULADA_A]->(Patente)       tipo A=Alteração, I=Incorporação

Estratégia:
  - Lê do PostgreSQL via psycopg2 com RealDictCursor
  - Escreve no Neo4j em batches via driver bolt (UNWIND $rows … MERGE)
  - Idempotente: MERGE em todos os nós e relacionamentos — seguro rerodar
  - Empresa não encontrada no Neo4j → DEPOSITOU simplesmente não é criado
    (CNPJ data pode estar em outro reference_month ou não ter sido carregado)
"""

from __future__ import annotations

import hashlib
import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

BATCH_SIZE = 500   # linhas por transação Neo4j


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )


def _get_neo4j_driver():
    from neo4j import GraphDatabase  # type: ignore
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(
            os.getenv("NEO4J_USER", "neo4j"),
            os.getenv("NEO4J_PASSWORD", "osint_graph_password"),
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pf_id(nome: str, cpf_mascara: str | None) -> str:
    """Stable SHA-256 identity key for pessoa física depositante."""
    key = f"{(nome or '').upper().strip()}|{cpf_mascara or ''}"
    return hashlib.sha256(key.encode()).hexdigest()


def _run_batches(driver, rows: list[dict], cypher: str) -> int:
    """Execute a MERGE cypher in BATCH_SIZE chunks, return total matched/merged."""
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        with driver.session() as session:
            result = session.run(cypher, rows=batch)
            record = result.single()
            total += int(record[0]) if record else 0
    return total


# ---------------------------------------------------------------------------
# Schema (constraints + indexes)
# ---------------------------------------------------------------------------

def ensure_inpi_schema() -> dict:
    """
    Create all Neo4j constraints and indexes for the INPI patent graph.
    IF NOT EXISTS makes every statement idempotent.
    """
    statements = [
        # Patente
        "CREATE CONSTRAINT patente_codigo_interno IF NOT EXISTS FOR (p:Patente) REQUIRE p.codigo_interno IS UNIQUE",
        "CREATE INDEX patente_numero_inpi IF NOT EXISTS FOR (p:Patente) ON (p.numero_inpi)",
        "CREATE INDEX patente_tipo IF NOT EXISTS FOR (p:Patente) ON (p.tipo_patente)",
        "CREATE INDEX patente_data_deposito IF NOT EXISTS FOR (p:Patente) ON (p.data_deposito)",
        "CREATE INDEX patente_data_publicacao IF NOT EXISTS FOR (p:Patente) ON (p.data_publicacao)",
        "CREATE INDEX patente_snapshot IF NOT EXISTS FOR (p:Patente) ON (p.snapshot_date)",
        "CREATE FULLTEXT INDEX patente_titulo IF NOT EXISTS FOR (p:Patente) ON EACH [p.titulo]",
        # ClasseIPC
        "CREATE CONSTRAINT ipc_simbolo IF NOT EXISTS FOR (c:ClasseIPC) REQUIRE c.simbolo IS UNIQUE",
        "CREATE INDEX ipc_grupo IF NOT EXISTS FOR (c:ClasseIPC) ON (c.grupo)",
        # DepositantePF
        "CREATE CONSTRAINT depositante_pf_id IF NOT EXISTS FOR (d:DepositantePF) REQUIRE d.depositante_id IS UNIQUE",
        "CREATE INDEX depositante_pf_nome IF NOT EXISTS FOR (d:DepositantePF) ON (d.nome)",
        "CREATE FULLTEXT INDEX depositante_pf_nome_ft IF NOT EXISTS FOR (d:DepositantePF) ON EACH [d.nome]",
        # Relationships
        "CREATE INDEX rel_depositou_ordem IF NOT EXISTS FOR ()-[r:DEPOSITOU]-() ON (r.ordem)",
        "CREATE INDEX rel_vinculada_tipo IF NOT EXISTS FOR ()-[r:VINCULADA_A]-() ON (r.tipo_vinculo)",
    ]

    driver = _get_neo4j_driver()
    created = 0
    try:
        with driver.session() as session:
            for stmt in statements:
                try:
                    session.run(stmt)  # type: ignore[arg-type]
                    created += 1
                except Exception as exc:
                    logger.warning("Schema stmt warning: %s → %s", stmt[:60], exc)

            result = session.run(
                "SHOW INDEXES YIELD name RETURN count(name) AS n"
            )
            total_indexes = (result.single() or {}).get("n", 0)

        logger.info("INPI schema ready: %d total indexes in Neo4j", total_indexes)
    finally:
        driver.close()

    return {"statements_executed": created, "total_indexes": total_indexes}


# ---------------------------------------------------------------------------
# Task 1 — MERGE :Patente nodes
# ---------------------------------------------------------------------------

def merge_patentes() -> dict:
    """
    MERGE one :Patente node per patent.
    Joins dados_bibliograficos with conteudo (LEFT JOIN) for the title.
    Stores dates as ISO strings (Neo4j Date type via the driver).
    """
    pg = _get_pg_conn()
    try:
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    b.codigo_interno,
                    b.numero_inpi,
                    b.tipo_patente,
                    b.data_deposito::text       AS data_deposito,
                    b.data_protocolo::text      AS data_protocolo,
                    b.data_publicacao::text     AS data_publicacao,
                    b.snapshot_date::text       AS snapshot_date,
                    c.titulo
                FROM inpi.patentes_dados_bibliograficos b
                LEFT JOIN inpi.patentes_conteudo c USING (codigo_interno)
                ORDER BY b.codigo_interno
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    logger.info("Merging %d :Patente nodes…", len(rows))

    cypher = """
        UNWIND $rows AS row
        MERGE (p:Patente {codigo_interno: row.codigo_interno})
        SET
            p.numero_inpi    = row.numero_inpi,
            p.tipo_patente   = row.tipo_patente,
            p.data_deposito  = row.data_deposito,
            p.data_protocolo = row.data_protocolo,
            p.data_publicacao= row.data_publicacao,
            p.snapshot_date  = row.snapshot_date,
            p.titulo         = row.titulo
        RETURN count(p) AS n
    """
    driver = _get_neo4j_driver()
    try:
        total = _run_batches(driver, rows, cypher)
    finally:
        driver.close()

    logger.info("✓ MERGEd %d :Patente nodes", total)
    return {"nodes_merged": total}


# ---------------------------------------------------------------------------
# Task 2a — MERGE :ClasseIPC + (Patente)-[:CLASSIFICADA_COMO]->(ClasseIPC)
# ---------------------------------------------------------------------------

def merge_ipc_and_classify() -> dict:
    """
    1. MERGE unique :ClasseIPC nodes (simbolo, grupo=LEFT(simbolo,4)).
    2. MERGE (Patente)-[:CLASSIFICADA_COMO]->(ClasseIPC) for every row.
    """
    pg = _get_pg_conn()
    try:
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Step 1: distinct IPC symbols
            cur.execute("""
                SELECT DISTINCT simbolo
                FROM inpi.patentes_classificacao_ipc
                WHERE simbolo IS NOT NULL AND simbolo <> ''
            """)
            ipc_rows = [dict(r) for r in cur.fetchall()]

            # Step 2: all patent→IPC rows
            cur.execute("""
                SELECT codigo_interno, simbolo, ordem, versao
                FROM inpi.patentes_classificacao_ipc
                WHERE simbolo IS NOT NULL AND simbolo <> ''
                ORDER BY codigo_interno, ordem
            """)
            rel_rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    driver = _get_neo4j_driver()
    try:
        # MERGE ClasseIPC nodes
        logger.info("Merging %d :ClasseIPC nodes…", len(ipc_rows))
        ipc_cypher = """
            UNWIND $rows AS row
            MERGE (c:ClasseIPC {simbolo: row.simbolo})
            SET c.grupo = LEFT(row.simbolo, 4)
            RETURN count(c) AS n
        """
        ipc_merged = _run_batches(driver, ipc_rows, ipc_cypher)
        logger.info("✓ MERGEd %d :ClasseIPC nodes", ipc_merged)

        # MERGE CLASSIFICADA_COMO relationships
        logger.info("Merging %d CLASSIFICADA_COMO relationships…", len(rel_rows))
        rel_cypher = """
            UNWIND $rows AS row
            MATCH (p:Patente {codigo_interno: row.codigo_interno})
            MATCH (c:ClasseIPC {simbolo: row.simbolo})
            MERGE (p)-[r:CLASSIFICADA_COMO]->(c)
            SET r.ordem = row.ordem, r.versao = row.versao
            RETURN count(r) AS n
        """
        rels_merged = _run_batches(driver, rel_rows, rel_cypher)
        logger.info("✓ MERGEd %d CLASSIFICADA_COMO relationships", rels_merged)
    finally:
        driver.close()

    return {"ipc_nodes": ipc_merged, "classificada_rels": rels_merged}


# ---------------------------------------------------------------------------
# Task 2b — MERGE :DepositantePF + (DepositantePF)-[:DEPOSITOU]->(Patente)
# ---------------------------------------------------------------------------

def merge_pf_and_deposito() -> dict:
    """
    1. MERGE :DepositantePF nodes (one per unique nome+cpf_mascara pair).
    2. MERGE (DepositantePF)-[:DEPOSITOU]->(Patente) for every PF row.
    """
    pg = _get_pg_conn()
    try:
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    codigo_interno,
                    ordem,
                    depositante      AS nome,
                    cgccpfdepositante AS cpf_mascara
                FROM inpi.patentes_depositantes
                WHERE tipopessoadepositante = 'pessoa física'
                ORDER BY codigo_interno, ordem
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    # Enrich with stable identity key
    for r in rows:
        r["depositante_id"] = _pf_id(r["nome"] or "", r["cpf_mascara"])
        r["nome"] = (r["nome"] or "").upper().strip()

    # Distinct nodes
    seen: dict[str, dict] = {}
    for r in rows:
        if r["depositante_id"] not in seen:
            seen[r["depositante_id"]] = {
                "depositante_id": r["depositante_id"],
                "nome": r["nome"],
                "cpf_mascara": r["cpf_mascara"],
            }
    pf_nodes = list(seen.values())

    driver = _get_neo4j_driver()
    try:
        # MERGE DepositantePF nodes
        logger.info("Merging %d :DepositantePF nodes…", len(pf_nodes))
        node_cypher = """
            UNWIND $rows AS row
            MERGE (d:DepositantePF {depositante_id: row.depositante_id})
            SET d.nome = row.nome, d.cpf_mascara = row.cpf_mascara
            RETURN count(d) AS n
        """
        nodes_merged = _run_batches(driver, pf_nodes, node_cypher)
        logger.info("✓ MERGEd %d :DepositantePF nodes", nodes_merged)

        # MERGE DEPOSITOU relationships
        logger.info("Merging %d PF DEPOSITOU relationships…", len(rows))
        rel_cypher = """
            UNWIND $rows AS row
            MATCH (d:DepositantePF {depositante_id: row.depositante_id})
            MATCH (p:Patente {codigo_interno: row.codigo_interno})
            MERGE (d)-[r:DEPOSITOU]->(p)
            SET r.ordem = row.ordem
            RETURN count(r) AS n
        """
        rels_merged = _run_batches(driver, rows, rel_cypher)
        logger.info("✓ MERGEd %d PF DEPOSITOU relationships", rels_merged)
    finally:
        driver.close()

    return {"pf_nodes": nodes_merged, "pf_depositou_rels": rels_merged}


# ---------------------------------------------------------------------------
# Task 2c — (Empresa)-[:DEPOSITOU]->(Patente)  via cnpj_basico_resolved
# ---------------------------------------------------------------------------

def link_empresa_deposito() -> dict:
    """
    MERGE (Empresa)-[:DEPOSITOU]->(Patente) for every PJ depositante row
    that has cnpj_basico_resolved populated.

    Uses MATCH on both sides — if the Empresa node doesn't exist in Neo4j
    (e.g. CNPJ data not yet loaded) the relationship is simply skipped.
    """
    pg = _get_pg_conn()
    try:
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    cnpj_basico_resolved AS cnpj_basico,
                    codigo_interno,
                    ordem,
                    data_inicio::text AS data_inicio,
                    data_fim::text    AS data_fim
                FROM inpi.patentes_depositantes
                WHERE cnpj_basico_resolved IS NOT NULL
                ORDER BY codigo_interno, ordem
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    logger.info("Merging %d Empresa DEPOSITOU relationships…", len(rows))

    cypher = """
        UNWIND $rows AS row
        MATCH (e:Empresa {cnpj_basico: row.cnpj_basico})
        MATCH (p:Patente {codigo_interno: row.codigo_interno})
        MERGE (e)-[r:DEPOSITOU]->(p)
        SET r.ordem = row.ordem,
            r.data_inicio = row.data_inicio,
            r.data_fim    = row.data_fim
        RETURN count(r) AS n
    """
    driver = _get_neo4j_driver()
    try:
        total = _run_batches(driver, rows, cypher)
    finally:
        driver.close()

    logger.info("✓ MERGEd %d Empresa DEPOSITOU relationships", total)
    return {"empresa_depositou_rels": total}


# ---------------------------------------------------------------------------
# Task 2d — (Patente)-[:VINCULADA_A]->(Patente)
# ---------------------------------------------------------------------------

def link_vinculos() -> dict:
    """
    MERGE (Patente)-[:VINCULADA_A]->(Patente) from patentes_vinculos.
    tipo_vinculo: A = Alteração, I = Incorporação.
    Both sides must already exist as :Patente nodes.
    """
    pg = _get_pg_conn()
    try:
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    codigo_interno_derivado AS codigo_derivado,
                    codigo_interno_origem   AS codigo_origem,
                    tipo_vinculo,
                    data_vinculo::text      AS data_vinculo
                FROM inpi.patentes_vinculos
                ORDER BY codigo_interno_derivado
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    logger.info("Merging %d VINCULADA_A relationships…", len(rows))

    cypher = """
        UNWIND $rows AS row
        MATCH (derivado:Patente {codigo_interno: row.codigo_derivado})
        MATCH (origem:Patente   {codigo_interno: row.codigo_origem})
        MERGE (derivado)-[r:VINCULADA_A]->(origem)
        SET r.tipo_vinculo = row.tipo_vinculo,
            r.data_vinculo = row.data_vinculo
        RETURN count(r) AS n
    """
    driver = _get_neo4j_driver()
    try:
        total = _run_batches(driver, rows, cypher)
    finally:
        driver.close()

    logger.info("✓ MERGEd %d VINCULADA_A relationships", total)
    return {"vinculo_rels": total}


# ---------------------------------------------------------------------------
# Report helper
# ---------------------------------------------------------------------------

def build_report(task_results: dict) -> str:
    lines = [
        "=" * 65,
        "  INPI Patentes — Neo4j Load Report",
        "=" * 65,
        f"  {'Patente nodes merged':<45} {task_results.get('nodes_merged', '?'):>10,}",
        f"  {'ClasseIPC nodes merged':<45} {task_results.get('ipc_nodes', '?'):>10,}",
        f"  {'DepositantePF nodes merged':<45} {task_results.get('pf_nodes', '?'):>10,}",
        "-" * 65,
        f"  {'CLASSIFICADA_COMO rels':<45} {task_results.get('classificada_rels', '?'):>10,}",
        f"  {'PF DEPOSITOU rels':<45} {task_results.get('pf_depositou_rels', '?'):>10,}",
        f"  {'Empresa DEPOSITOU rels':<45} {task_results.get('empresa_depositou_rels', '?'):>10,}",
        f"  {'VINCULADA_A rels':<45} {task_results.get('vinculo_rels', '?'):>10,}",
        "=" * 65,
    ]
    return "\n".join(lines)
