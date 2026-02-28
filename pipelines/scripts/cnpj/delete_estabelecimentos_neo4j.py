"""
Cenário A — Deleção de nós :Estabelecimento e relações [:PERTENCE_A] do Neo4j

Executa a remoção em lotes pequenos via APOC para não saturar a memória do Neo4j
nem os transaction logs do SSD.

Uso:
    docker exec -it osint-airflow-worker bash
    python /opt/airflow/scripts/cnpj/delete_estabelecimentos_neo4j.py

Variáveis de ambiente (opcionais — usa defaults do docker-compose):
    NEO4J_URI        bolt://neo4j:7687
    NEO4J_USER       neo4j
    NEO4J_PASSWORD   osint_graph_password

O script é idempotente: pode ser interrompido e reexecutado sem efeitos colaterais.
"""

import os
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
logger = logging.getLogger(__name__)

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "osint_graph_password")

BATCH_SIZE   = 10_000   # nós por transação — ajuste se necessário
REPORT_EVERY = 10       # loga progresso a cada N lotes


def count_nodes(session) -> int:
    result = session.run("MATCH (e:Estabelecimento) RETURN count(e) AS n")
    return result.single()["n"]


def delete_batch(tx, batch_size: int) -> int:
    """
    Deleta um lote de nós :Estabelecimento usando DETACH DELETE.
    Retorna o número de nós deletados neste lote.
    """
    result = tx.run(
        """
        MATCH (est:Estabelecimento)
        WITH est LIMIT $batch_size
        DETACH DELETE est
        RETURN count(*) AS deleted
        """,
        batch_size=batch_size,
    )
    return result.single()["deleted"]


def main():
    from neo4j import GraphDatabase

    logger.info("Conectando ao Neo4j: %s", NEO4J_URI)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        with driver.session() as session:
            # 1. Contagem inicial
            total_before = count_nodes(session)
            if total_before == 0:
                logger.info("Nenhum nó :Estabelecimento encontrado. Nada a fazer.")
                return

            logger.info("Nós :Estabelecimento antes da deleção: %s", f"{total_before:,}")
            logger.info("Batch size: %s | APOC não necessário — usando DETACH DELETE com LIMIT", f"{BATCH_SIZE:,}")
            logger.info("Iniciando deleção em lotes...")

            total_deleted = 0
            batch_num = 0
            start_time = time.time()

            while True:
                batch_start = time.time()
                deleted = session.execute_write(delete_batch, BATCH_SIZE)

                if deleted == 0:
                    break  # Não há mais nós para deletar

                total_deleted += deleted
                batch_num += 1
                batch_duration = time.time() - batch_start

                if batch_num % REPORT_EVERY == 0:
                    elapsed = time.time() - start_time
                    remaining = total_before - total_deleted
                    rate = total_deleted / elapsed if elapsed > 0 else 0
                    eta_sec = remaining / rate if rate > 0 else 0
                    logger.info(
                        "Lote %d | Deletados: %s / %s (%.1f%%) | "
                        "Taxa: %.0f/s | ETA: %.0fmin",
                        batch_num,
                        f"{total_deleted:,}",
                        f"{total_before:,}",
                        total_deleted * 100 / total_before,
                        rate,
                        eta_sec / 60,
                    )

            # 2. Validação final
            total_after = count_nodes(session)
            total_duration = time.time() - start_time

            logger.info("=" * 60)
            logger.info("Deleção concluída!")
            logger.info("  Nós antes  : %s", f"{total_before:,}")
            logger.info("  Deletados  : %s", f"{total_deleted:,}")
            logger.info("  Nós restantes: %s", f"{total_after:,}")
            logger.info("  Tempo total: %.1f min", total_duration / 60)

            if total_after > 0:
                logger.warning(
                    "%s nós :Estabelecimento ainda existem — execute o script novamente.",
                    f"{total_after:,}",
                )

            # 3. Remover índice e constraint de :Estabelecimento (libera espaço no SSD)
            logger.info("Removendo índices de :Estabelecimento...")
            index_queries = [
                "DROP CONSTRAINT estabelecimento_cnpj_unique IF EXISTS",
                "DROP INDEX estabelecimento_cnpj IF EXISTS",
            ]
            for q in index_queries:
                try:
                    session.run(q)
                    logger.info("  OK: %s", q)
                except Exception as e:
                    logger.debug("  Índice/constraint não encontrado (ignorado): %s — %s", q, e)

            logger.info("Pronto. Neo4j agora contém apenas :Empresa, :Pessoa e [:SOCIO_DE].")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
