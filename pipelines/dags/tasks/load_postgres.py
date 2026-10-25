"""CNPJ load step: Parquet → staging table → deduplicated upsert into the cnpj schema."""

import logging
import os
import sys
import threading
import time
from contextlib import contextmanager

import duckdb
import psycopg2

from .config import PROCESSED_PATH, TEMP_SSD_PATH

sys.path.insert(0, "/opt/airflow/scripts/cnpj")

logger = logging.getLogger(__name__)

# table: (parquet glob, conflict key). socio has no natural key: its month is replaced instead.
TABLES = {
    "cnae": ("cnaes.parquet", "cnae_fiscal"),
    "motivo_situacao_cadastral": ("motivos.parquet", "codigo"),
    "municipio": ("municipios.parquet", "codigo"),
    "natureza_juridica": ("naturezas.parquet", "codigo"),
    "pais": ("paises.parquet", "codigo"),
    "qualificacao_socio": ("qualificacoes.parquet", "codigo"),
    "empresa": ("empresas_*.parquet", "cnpj_basico"),
    "simples_nacional": ("simples.parquet", "cnpj_basico"),
    "estabelecimento": ("estabelecimentos_*.parquet", "cnpj_basico, cnpj_ordem, cnpj_dv"),
    "socio": ("socios_*.parquet", None),
}
REFERENCE_TABLES = ("cnae", "motivo_situacao_cadastral", "municipio", "natureza_juridica", "pais", "qualificacao_socio")

# Dropped before the bulk load and rebuilt afterwards; maintaining them row by row is far slower.
HEAVY_INDEXES = {
    "empresa": [
        ("idx_empresa_razao_social", "USING GIN (to_tsvector('portuguese', razao_social))"),
        ("idx_empresa_porte", "(porte_empresa)"),
        ("idx_empresa_natureza", "(natureza_juridica)"),
        ("idx_empresa_ref_month", "(reference_month)"),
    ],
    "estabelecimento": [
        ("idx_estabelecimento_nome_fantasia", "USING GIN (to_tsvector('portuguese', nome_fantasia))"),
        ("idx_estabelecimento_situacao", "(situacao_cadastral)"),
        ("idx_estabelecimento_municipio", "(codigo_municipio)"),
        ("idx_estabelecimento_uf", "(uf)"),
        ("idx_estabelecimento_cnae", "(cnae_fiscal_principal)"),
        ("idx_estabelecimento_ref_month", "(reference_month)"),
    ],
    "socio": [
        ("idx_socio_cnpj_basico", "(cnpj_basico)"),
        ("idx_socio_cpf_cnpj", "(cpf_cnpj_socio)"),
        ("idx_socio_nome", "USING GIN (to_tsvector('portuguese', nome_socio_razao_social)) WHERE nome_socio_razao_social IS NOT NULL"),
        ("idx_socio_ref_month", "(reference_month)"),
        ("idx_socio_qualificacao", "(qualificacao_socio)"),
    ],
}


@contextmanager
def _phase(description: str, heartbeat_seconds: int = 120):
    """Log when a long step starts, every couple of minutes while it runs, and when it ends."""
    start = time.time()
    finished = threading.Event()

    def heartbeat():
        while not finished.wait(heartbeat_seconds):
            logger.info(f"  {description}: still running ({time.time() - start:.0f}s)")

    logger.info(f"  {description}...")
    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        yield
    finally:
        finished.set()
    logger.info(f"  {description}: done in {time.time() - start:.1f}s")


def _pg_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "osint_metadata"),
        "user": os.getenv("POSTGRES_USER", "osint_admin"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def _parquet_columns(path: str) -> list[str]:
    with duckdb.connect() as duck:
        return [row[0] for row in duck.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]


def _stage(table: str, files: list[str], reference_month: str) -> dict[str, int]:
    """Copy every Parquet file into cnpj.<table>_staging through DuckDB; returns rows per file."""
    spill_dir = TEMP_SSD_PATH / "duckdb_tmp"
    spill_dir.mkdir(parents=True, exist_ok=True)
    duck = duckdb.connect(config={"temp_directory": str(spill_dir), "memory_limit": "4GB"})
    rows_per_file = {}
    try:
        duck.execute("INSTALL postgres; LOAD postgres; SET pg_null_byte_replacement=''")
        dsn = " ".join(f"{k}={v}" for k, v in _pg_params().items())
        duck.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES)")
        month_cols, month_vals = ("", "") if table in REFERENCE_TABLES else (", reference_month", f", '{reference_month}'")
        for path in files:
            cols = ", ".join(_parquet_columns(path))
            start = time.time()
            duck.execute(f"INSERT INTO pg.cnpj.{table}_staging ({cols}{month_cols}) SELECT {cols}{month_vals} FROM '{path}'")
            rows_per_file[path] = duck.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
            logger.info(f"  staged {os.path.basename(path)}: {rows_per_file[path]:,} rows in {time.time() - start:.1f}s")
    finally:
        duck.close()
    return rows_per_file


def load_table(table: str, reference_month: str) -> int:
    """Replace the month's data in cnpj.<table> with the Parquet output of the transform."""
    from manifest_tracker import mark_loaded_postgres_by_parquet_path

    pattern, key = TABLES[table]
    files = sorted(str(p) for p in (PROCESSED_PATH / reference_month).glob(pattern) if p.stat().st_size > 0)
    if not files:
        raise FileNotFoundError(f"No {pattern} for {reference_month} in {PROCESSED_PATH}")
    logger.info(f"Loading cnpj.{table} from {len(files)} file(s) for {reference_month}")

    # Staging holds only the loaded columns: DuckDB writes NULL into any other column, which
    # would bypass defaults such as socio.id.
    columns = _parquet_columns(files[0]) + ([] if table in REFERENCE_TABLES else ["reference_month"])
    col_list = ", ".join(columns)

    conn = psycopg2.connect(**_pg_params())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS cnpj.{table}_staging")
            cur.execute(f"CREATE UNLOGGED TABLE cnpj.{table}_staging AS SELECT {col_list} FROM cnpj.{table} WITH NO DATA")
            for name, _ in HEAVY_INDEXES.get(table, []):
                cur.execute(f"DROP INDEX IF EXISTS cnpj.{name}")

        rows_per_file = _stage(table, files, reference_month)

        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'cnpj' AND table_name = %s AND column_name = 'updated_at'",
                (table,),
            )
            has_updated_at = cur.fetchone() is not None
            staged = sum(rows_per_file.values())
            if key is None:
                with _phase(f"replacing {reference_month} in cnpj.{table} with {staged:,} staged rows"):
                    cur.execute(f"DELETE FROM cnpj.{table} WHERE reference_month = %s", (reference_month,))
                    cur.execute(f"INSERT INTO cnpj.{table} ({col_list}) SELECT {col_list} FROM cnpj.{table}_staging")
            else:
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
                if has_updated_at:
                    updates += ", updated_at = CURRENT_TIMESTAMP"
                # Receita occasionally publishes a key twice (e.g. a real company plus an empty
                # placeholder row); keep the most complete row per key.
                with _phase(f"deduplicating and upserting {staged:,} staged rows into cnpj.{table}"):
                    cur.execute(f"""
                        INSERT INTO cnpj.{table} ({col_list})
                        SELECT DISTINCT ON ({key}) {col_list}
                        FROM cnpj.{table}_staging
                        ORDER BY {key}, num_nonnulls({col_list}) DESC
                        ON CONFLICT ({key}) DO UPDATE SET {updates}
                    """)
            written = cur.rowcount
            cur.execute(f"DROP TABLE cnpj.{table}_staging")
            with _phase("committing"):
                conn.commit()
        logger.info(f"  wrote {written:,} rows into cnpj.{table}")

        conn.autocommit = True
        with conn.cursor() as cur:
            for name, definition in HEAVY_INDEXES.get(table, []):
                with _phase(f"building index {name}"):
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON cnpj.{table} {definition}")
            with _phase(f"analyzing cnpj.{table}"):
                cur.execute(f"ANALYZE cnpj.{table}")
    finally:
        conn.close()

    for path, rows in rows_per_file.items():
        mark_loaded_postgres_by_parquet_path(path, rows)
    return written


def refresh_search_views() -> None:
    """Rebuild the company search materialized views (concurrently once populated)."""
    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            cur.execute("SET max_parallel_workers_per_gather = 0")
            for view in ("mv_company_search", "mv_company_search_inactive"):
                cur.execute(
                    "SELECT relispopulated FROM pg_class WHERE relname = %s AND relnamespace = 'cnpj'::regnamespace",
                    (view,),
                )
                populated = cur.fetchone()[0]
                with _phase(f"refreshing cnpj.{view}{' concurrently' if populated else ''}"):
                    cur.execute(f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY ' if populated else ''}cnpj.{view}")
                    cur.execute(f"ANALYZE cnpj.{view}")
                cur.execute(f"SELECT count(*), pg_size_pretty(pg_total_relation_size('cnpj.{view}')) FROM cnpj.{view}")
                rows, size = cur.fetchone()
                logger.info(f"cnpj.{view}: {rows:,} rows, {size}")
    finally:
        conn.close()
