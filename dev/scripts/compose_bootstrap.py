#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2


ROOT_DIR = Path("/workspace")
DEV_DIR = ROOT_DIR / "dev"
SEED_SQL_PATH = DEV_DIR / "data/postgres/seed_sample.sql"
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


def run_postgres_sql_file(connection, path: Path) -> None:
    log(f"Applying PostgreSQL seed from {path}.")
    with connection.cursor() as cursor:
        cursor.execute(path.read_text(encoding="utf-8"))
    connection.commit()


def ensure_postgres_sample(connection) -> None:
    empresa_count = count_rows(connection, "cnpj.empresa")
    estabelecimento_count = count_rows(connection, "cnpj.estabelecimento")
    search_count = count_rows(connection, "cnpj.company_search")

    if empresa_count == 0 or estabelecimento_count == 0 or search_count == 0:
        run_postgres_sql_file(connection, SEED_SQL_PATH)
    else:
        log("PostgreSQL CNPJ sample already available. Skipping seed reload.")

    empresa_count = count_rows(connection, "cnpj.empresa")
    estabelecimento_count = count_rows(connection, "cnpj.estabelecimento")
    search_count = count_rows(connection, "cnpj.company_search")

    if empresa_count == 0 or estabelecimento_count == 0 or search_count == 0:
        raise RuntimeError(
            "PostgreSQL bootstrap validation failed: CNPJ sample or company search is empty. "
            f"empresas={empresa_count}, estabelecimentos={estabelecimento_count}, "
            f"company_search={search_count}"
        )

    log(
        "PostgreSQL CNPJ sample ready. "
        f"empresas={empresa_count}, estabelecimentos={estabelecimento_count}, "
        f"company_search={search_count}"
    )


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

    postgres_connection = wait_for_postgres()

    try:
        ensure_postgres_sample(postgres_connection)
        ensure_geo_dataset()
        ensure_finep_dataset(postgres_connection)
    finally:
        postgres_connection.close()

    log("Bootstrap complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())