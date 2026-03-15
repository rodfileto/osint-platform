#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from neo4j import GraphDatabase


ROOT_DIR = Path("/workspace")
DEV_DIR = ROOT_DIR / "dev"
SEED_SQL_PATH = DEV_DIR / "data/postgres/seed_sample.sql"
NEO4J_SCHEMA_PATH = ROOT_DIR / "infrastructure/neo4j/init-cnpj-schema.cypher"
NEO4J_SEED_PATH = DEV_DIR / "data/neo4j/seed_sample.cypher"
GEO_BOOTSTRAP_SCRIPT = ROOT_DIR / "pipelines/scripts/geo/load_br_geo_reference.py"
FINEP_BOOTSTRAP_SCRIPT = ROOT_DIR / "pipelines/scripts/finep/bootstrap_full_load.py"
FINEP_OUTPUT_DIR = Path(os.getenv("DEV_BOOTSTRAP_FINEP_OUTPUT_DIR", "/bootstrap-data/finep/raw"))


def log(message: str) -> None:
    print(f"[dev-bootstrap] {message}", flush=True)


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def split_cypher_statements(path: Path) -> list[str]:
    statements: list[str] = []
    current: list[str] = []

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        current.append(raw_line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip()
            statements.append(statement[:-1].strip())
            current = []

    if current:
        statement = "\n".join(current).strip()
        if statement:
            statements.append(statement)

    return statements


def wait_for_postgres(max_attempts: int = 60, delay_seconds: int = 2):
    for attempt in range(1, max_attempts + 1):
        try:
            connection = psycopg2.connect(
                host=os.environ["POSTGRES_HOST"],
                port=os.environ["POSTGRES_PORT"],
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            log("PostgreSQL is ready.")
            return connection
        except psycopg2.Error as exc:
            if attempt == max_attempts:
                raise RuntimeError("PostgreSQL did not become ready in time.") from exc
            log(f"Waiting for PostgreSQL ({attempt}/{max_attempts}): {exc.pgerror or exc}")
            time.sleep(delay_seconds)


def wait_for_neo4j(max_attempts: int = 60, delay_seconds: int = 2):
    uri = os.environ["NEO4J_URI"]
    auth = (os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"])

    for attempt in range(1, max_attempts + 1):
        driver = GraphDatabase.driver(uri, auth=auth)
        try:
            with driver.session() as session:
                session.run("RETURN 1").consume()
            log("Neo4j is ready.")
            return driver
        except Exception as exc:
            driver.close()
            if attempt == max_attempts:
                raise RuntimeError("Neo4j did not become ready in time.") from exc
            log(f"Waiting for Neo4j ({attempt}/{max_attempts}): {exc}")
            time.sleep(delay_seconds)


def query_single_row(connection, query: str) -> tuple:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Query returned no rows: {query}")
        return row


def count_rows(connection, table_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return int(cursor.fetchone()[0])


def get_matview_state(connection, view_name: str) -> tuple[bool, int]:
    query = f"""
        SELECT relispopulated
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'cnpj' AND c.relname = '{view_name}'
    """
    row = query_single_row(connection, query)
    is_populated = bool(row[0])
    row_count = count_rows(connection, f"cnpj.{view_name}") if is_populated else 0
    return is_populated, row_count


def run_postgres_sql_file(connection, path: Path) -> None:
    log(f"Applying PostgreSQL seed from {path}.")
    with connection.cursor() as cursor:
        cursor.execute(path.read_text(encoding="utf-8"))
    connection.commit()


def refresh_cnpj_matviews(connection) -> None:
    log("Refreshing CNPJ materialized views.")
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW cnpj.mv_company_search;")
        cursor.execute("REFRESH MATERIALIZED VIEW cnpj.mv_company_search_inactive;")
        cursor.execute("ANALYZE cnpj.mv_company_search;")
        cursor.execute("ANALYZE cnpj.mv_company_search_inactive;")
    connection.commit()


def ensure_postgres_sample(connection) -> None:
    empresa_count = count_rows(connection, "cnpj.empresa")
    estabelecimento_count = count_rows(connection, "cnpj.estabelecimento")
    active_populated, active_count = get_matview_state(connection, "mv_company_search")

    if empresa_count == 0 or estabelecimento_count == 0:
        run_postgres_sql_file(connection, SEED_SQL_PATH)
    elif not active_populated or active_count == 0:
        refresh_cnpj_matviews(connection)
    else:
        log("PostgreSQL CNPJ sample already available. Skipping seed reload.")

    empresa_count = count_rows(connection, "cnpj.empresa")
    estabelecimento_count = count_rows(connection, "cnpj.estabelecimento")
    active_populated, active_count = get_matview_state(connection, "mv_company_search")
    inactive_populated, inactive_count = get_matview_state(connection, "mv_company_search_inactive")

    if empresa_count == 0 or estabelecimento_count == 0 or not active_populated or active_count == 0:
        raise RuntimeError(
            "PostgreSQL bootstrap validation failed: CNPJ sample or active materialized view is empty. "
            f"empresas={empresa_count}, estabelecimentos={estabelecimento_count}, "
            f"mv_company_search={active_count}, mv_company_search_inactive={inactive_count}"
        )

    log(
        "PostgreSQL CNPJ sample ready. "
        f"empresas={empresa_count}, estabelecimentos={estabelecimento_count}, "
        f"mv_company_search={active_count}, mv_company_search_inactive={inactive_count}, "
        f"inactive_populated={inactive_populated}"
    )


def run_cypher_file(driver, path: Path) -> None:
    statements = split_cypher_statements(path)
    if not statements:
        return

    with driver.session() as session:
        for statement in statements:
            session.run(statement).consume()


def neo4j_node_count(driver, label: str) -> int:
    with driver.session() as session:
        result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
        return int(result.single()["count"])


def ensure_neo4j_sample(driver) -> None:
    log("Applying Neo4j schema.")
    run_cypher_file(driver, NEO4J_SCHEMA_PATH)

    empresa_count = neo4j_node_count(driver, "Empresa")
    pessoa_count = neo4j_node_count(driver, "Pessoa")
    if empresa_count == 0 or pessoa_count == 0:
        log("Loading Neo4j sample graph.")
        run_cypher_file(driver, NEO4J_SEED_PATH)
        empresa_count = neo4j_node_count(driver, "Empresa")
        pessoa_count = neo4j_node_count(driver, "Pessoa")
    else:
        log("Neo4j sample graph already available. Skipping seed reload.")

    if empresa_count == 0:
        raise RuntimeError("Neo4j bootstrap validation failed: no Empresa nodes found.")

    log(f"Neo4j sample ready. empresas={empresa_count}, pessoas={pessoa_count}")


def finep_row_count(connection) -> int:
    query = """
        SELECT
            (SELECT COUNT(*) FROM finep.projetos_operacao_direta) +
            (SELECT COUNT(*) FROM finep.projetos_credito_descentralizado) +
            (SELECT COUNT(*) FROM finep.projetos_investimento) +
            (SELECT COUNT(*) FROM finep.projetos_ancine) +
            (SELECT COUNT(*) FROM finep.liberacoes_operacao_direta) +
            (SELECT COUNT(*) FROM finep.liberacoes_credito_descentralizado) +
            (SELECT COUNT(*) FROM finep.liberacoes_ancine)
    """
    return int(query_single_row(connection, query)[0])


def ensure_finep_dataset(connection) -> None:
    if not env_flag("DEV_BOOTSTRAP_FINEP", True):
        log("Skipping FINEP bootstrap because DEV_BOOTSTRAP_FINEP is disabled.")
        return

    current_rows = finep_row_count(connection)
    if current_rows > 0:
        log(f"FINEP dataset already available. rows={current_rows}")
        return

    log("Loading full FINEP dataset.")
    subprocess.run(
        [
            sys.executable,
            str(FINEP_BOOTSTRAP_SCRIPT),
            "--output-dir",
            str(FINEP_OUTPUT_DIR),
        ],
        check=True,
    )

    current_rows = finep_row_count(connection)
    if current_rows == 0:
        raise RuntimeError("FINEP bootstrap validation failed: no rows were loaded.")

    log(f"FINEP dataset ready. rows={current_rows}")


def ensure_geo_dataset() -> None:
    if not env_flag("DEV_BOOTSTRAP_GEO", True):
        log("Skipping geo bootstrap because DEV_BOOTSTRAP_GEO is disabled.")
        return

    if not GEO_BOOTSTRAP_SCRIPT.exists():
        raise FileNotFoundError(f"Missing geo bootstrap script: {GEO_BOOTSTRAP_SCRIPT}")

    log("Loading canonical Brazil geo reference data.")
    subprocess.run(
        [
            sys.executable,
            str(GEO_BOOTSTRAP_SCRIPT),
        ],
        check=True,
    )


def main() -> int:
    if not SEED_SQL_PATH.exists():
        raise FileNotFoundError(f"Missing PostgreSQL seed file: {SEED_SQL_PATH}")
    if not NEO4J_SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Missing Neo4j schema file: {NEO4J_SCHEMA_PATH}")
    if not NEO4J_SEED_PATH.exists():
        raise FileNotFoundError(f"Missing Neo4j seed file: {NEO4J_SEED_PATH}")

    postgres_connection = wait_for_postgres()
    neo4j_driver = wait_for_neo4j()

    try:
        ensure_postgres_sample(postgres_connection)
        ensure_geo_dataset()
        ensure_neo4j_sample(neo4j_driver)
        ensure_finep_dataset(postgres_connection)
    finally:
        neo4j_driver.close()
        postgres_connection.close()

    log("Bootstrap complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())