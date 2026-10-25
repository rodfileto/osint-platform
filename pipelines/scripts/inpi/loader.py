"""
INPI patent CSVs (MinIO snapshots) → inpi.* tables.

Each table is staged on its own: DuckDB reads the CSV, drops exact repeats (INPI's exports
repeat rows heavily) and writes inpi.<table>_staging. publish() then replaces all ten tables
in one transaction, keeping only rows whose patent exists in patentes_dados_bibliograficos.
"""

import logging
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Optional

import duckdb
import psycopg2

logger = logging.getLogger(__name__)

PARENT = "patentes_dados_bibliograficos"


def _str(col: str) -> str:
    return f"NULLIF(TRIM({col}), '')"


def _int(col: str) -> str:
    return f"TRY_CAST(TRY_CAST({_str(col)} AS DOUBLE) AS BIGINT)"


def _date(col: str) -> str:
    return f"TRY_CAST({_str(col)} AS DATE)"


def _cnpj_basico(document: str, person_type: str) -> str:
    """First 8 digits of a 14-digit CNPJ, only for legal entities."""
    return (f"CASE WHEN lower({person_type}) LIKE '%jur%' AND regexp_full_match(TRIM({document}), '\\d{{14}}') "
            f"THEN left(TRIM({document}), 8) END")


@dataclass(frozen=True)
class Table:
    columns: dict[str, str]                # table column → DuckDB expression over the CSV columns
    key: Optional[tuple[str, ...]] = None  # natural key: keep the most complete row per key
    parents: tuple[str, ...] = ("codigo_interno",)  # columns that must exist in the parent table


TABLES = {
    PARENT: Table(
        columns={
            "codigo_interno": _int("codigo_interno"),
            "numero_inpi": _str("numero_inpi"),
            "tipo_patente": f"NULLIF(upper(regexp_extract(TRIM(numero_inpi), '^([A-Za-z]+)', 1)), '')",
            "data_deposito": _date("data_deposito"),
            "data_protocolo": _date("data_protocolo"),
            "data_publicacao": _date("data_publicacao"),
            "numero_pct": _str("numero_pct"),
            "numero_wo": _str("numero_wo"),
            "data_publicacao_wo": _date("data_publicacao_wo"),
            "data_entrada_fase_nacional": _date("data_entrada_fase_nacional"),
            "sigilo": "COALESCE(TRY_CAST(sigilo AS DOUBLE) > 0, false)",
        },
        key=("codigo_interno",),
        parents=(),
    ),
    "patentes_conteudo": Table(
        columns={"codigo_interno": _int("codigo_interno"), "numero_inpi": _str("numero_inpi"),
                 "titulo": _str("titulo"), "resumo": _str("resumo")},
        key=("codigo_interno",),
    ),
    "patentes_inventores": Table(
        columns={"codigo_interno": _int("codigo_interno"), "ordem": _int("ordem"),
                 "autor": _str("autor"), "pais": _str("pais"), "estado": _str("estado")},
    ),
    # Successive holders of the same position are separate rows (data_inicio / data_fim).
    "patentes_depositantes": Table(
        columns={
            "codigo_interno": _int("codigo_interno"),
            "ordem": _int("ordem"),
            "pais": _str("pais"),
            "estado": _str("estado"),
            "depositante": _str("depositante"),
            "cgccpfdepositante": _str("cgccpfdepositante"),
            "tipopessoadepositante": _str("tipopessoadepositante"),
            "data_inicio": _date("data_inicio_depositante"),
            "data_fim": _date("data_fim_depositante"),
            "cnpj_basico_resolved": _cnpj_basico("cgccpfdepositante", "tipopessoadepositante"),
        },
    ),
    "patentes_classificacao_ipc": Table(
        columns={"codigo_interno": _int("codigo_interno"), "ordem": _int("ordem"),
                 "simbolo": _str("simbolo"), "versao": _str("versao")},
    ),
    "patentes_despachos": Table(
        columns={"codigo_interno": _int("codigo_interno"), "numero_rpi": _int("numero_rpi"),
                 "data_rpi": _date("data_rpi"), "codigo_despacho": _str("codigo_despacho"),
                 "complemento_despacho": _str("complemento_despacho")},
    ),
    "patentes_procuradores": Table(
        columns={
            "codigo_interno": _int("codigo_interno"),
            "procurador": _str("procurador"),
            "cgccpfprocurador": _str("cgccpfprocurador"),
            "tipopessoaprocurador": _str("tipopessoaprocurador"),
            "data_inicio": _date("data_inicio_procurador"),
            "cnpj_basico_resolved": _cnpj_basico("cgccpfprocurador", "tipopessoaprocurador"),
        },
    ),
    "patentes_prioridades": Table(
        columns={"codigo_interno": _int("codigo_interno"), "pais_prioridade": _str("pais_prioridade"),
                 "numero_prioridade": _str("numero_prioridade"), "data_prioridade": _date("data_prioridade")},
    ),
    "patentes_vinculos": Table(
        columns={"codigo_interno_derivado": _int("codigo_interno_derivado"),
                 "codigo_interno_origem": _int("codigo_interno_origem"),
                 "data_vinculo": _date("data_vinculo"), "tipo_vinculo": _str("tipo_vinculo")},
        key=("codigo_interno_derivado", "codigo_interno_origem", "tipo_vinculo"),
        parents=("codigo_interno_derivado", "codigo_interno_origem"),
    ),
    "patentes_renumeracoes": Table(
        columns={"codigo_interno": _int("codigo_interno"), "numero_inpi_original": _str("numero_inpi_original")},
        key=("codigo_interno", "numero_inpi_original"),
    ),
}


def _pg_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "osint_metadata"),
        "user": os.getenv("POSTGRES_USER", "osint_admin"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def _connect():
    return psycopg2.connect(**_pg_params())


def latest_snapshots() -> dict[str, dict]:
    """{dataset_name: {id, minio_bucket, minio_key, snapshot_date, processing_status}} for the newest snapshot."""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (dataset_name)
                    dataset_name, id, minio_bucket, minio_key, snapshot_date, processing_status
                FROM inpi.download_manifest
                WHERE processing_status IN ('downloaded', 'loaded')
                ORDER BY dataset_name, snapshot_date DESC, download_date DESC
            """)
            names = [d[0] for d in cur.description]
            return {row[0]: dict(zip(names, row)) for row in cur.fetchall()}
    finally:
        conn.close()


def has_pending_snapshot() -> bool:
    """True when the newest snapshot of some dataset has not been loaded yet."""
    snapshots = latest_snapshots()
    pending = sorted(n for n, s in snapshots.items() if s["processing_status"] == "downloaded")
    logger.info(f"{len(snapshots)} datasets with snapshots; not yet loaded: {pending or 'none'}")
    return bool(pending)


def stage_table(table: str) -> int:
    """Read the newest snapshot of the table's CSV into inpi.<table>_staging."""
    from inpi.downloader import _get_minio_s3_client

    spec = TABLES[table]
    snapshot = latest_snapshots().get(table)
    if snapshot is None:
        raise RuntimeError(f"No snapshot of {table} has been downloaded")

    columns = list(spec.columns) + ["snapshot_date"]
    selects = [f"{expr} AS {col}" for col, expr in spec.columns.items()]
    selects.append(f"DATE '{snapshot['snapshot_date']}' AS snapshot_date")

    with tempfile.TemporaryDirectory(dir="/opt/airflow/temp_ssd") as work:
        csv_path = os.path.join(work, "data.csv")
        _get_minio_s3_client().download_file(snapshot["minio_bucket"], snapshot["minio_key"], csv_path)

        duck = duckdb.connect(config={"temp_directory": work, "memory_limit": "4GB"})
        try:
            source = f"read_csv('{csv_path}', all_varchar=true, header=true)"
            duck.execute(f"""
                CREATE TABLE distinct_rows AS
                SELECT DISTINCT {', '.join(selects)} FROM {source}
                WHERE {next(iter(spec.columns.values()))} IS NOT NULL
            """)
            raw = duck.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
            distinct = duck.execute("SELECT count(*) FROM distinct_rows").fetchone()[0]
            final = "distinct_rows"
            note = ""
            if spec.key:
                key = ", ".join(spec.key)
                completeness = " + ".join(f"({c} IS NOT NULL)::INT" for c in columns)
                duck.execute(f"""
                    CREATE TABLE keyed_rows AS
                    SELECT DISTINCT ON ({key}) * FROM distinct_rows ORDER BY {key}, {completeness} DESC
                """)
                final = "keyed_rows"
                kept = duck.execute("SELECT count(*) FROM keyed_rows").fetchone()[0]
                note = f", {distinct - kept:,} rows sharing a key dropped (kept the most complete)"

            conn = _connect()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS inpi.{table}_staging")
                # Only the loaded columns: DuckDB writes NULL into any other column, bypassing defaults.
                cur.execute(f"CREATE UNLOGGED TABLE inpi.{table}_staging AS "
                            f"SELECT {', '.join(columns)} FROM inpi.{table} WITH NO DATA")
            conn.close()

            duck.execute("INSTALL postgres; LOAD postgres; SET pg_null_byte_replacement=''")
            dsn = " ".join(f"{k}={v}" for k, v in _pg_params().items())
            duck.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES)")
            start = time.time()
            duck.execute(f"INSERT INTO pg.inpi.{table}_staging ({', '.join(columns)}) SELECT {', '.join(columns)} FROM {final}")
            staged = duck.execute(f"SELECT count(*) FROM {final}").fetchone()[0]
        finally:
            duck.close()

    logger.info(f"{table}: {raw:,} CSV rows, {raw - distinct:,} exact repeats dropped{note}; "
                f"staged {staged:,} rows in {time.time() - start:.0f}s")
    return staged


def publish() -> dict[str, int]:
    """Replace all ten tables with their staging tables in one transaction."""
    snapshots = latest_snapshots()
    conn = _connect()
    counts = {}
    try:
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE " + ", ".join(f"inpi.{t}" for t in TABLES))
            for table, spec in TABLES.items():  # PARENT is first
                columns = ", ".join(list(spec.columns) + ["snapshot_date"])
                where = " AND ".join(
                    f"EXISTS (SELECT 1 FROM inpi.{PARENT} p WHERE p.codigo_interno = s.{col})" for col in spec.parents
                ) or "true"
                start = time.time()
                cur.execute(f"INSERT INTO inpi.{table} ({columns}) SELECT {columns} FROM inpi.{table}_staging s WHERE {where}")
                counts[table] = cur.rowcount
                cur.execute(f"SELECT count(*) FROM inpi.{table}_staging")
                orphans = cur.fetchone()[0] - counts[table]
                logger.info(f"{table}: {counts[table]:,} rows ({orphans:,} without a known patent) in {time.time() - start:.0f}s")
                cur.execute(f"DROP TABLE inpi.{table}_staging")
            cur.execute(
                "UPDATE inpi.download_manifest SET processing_status = 'loaded', updated_at = now() WHERE id = ANY(%s)",
                ([s["id"] for s in snapshots.values()],),
            )
    finally:
        conn.close()
    return counts


def refresh_search_view() -> None:
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT relispopulated FROM pg_class WHERE oid = 'inpi.mv_patent_search'::regclass")
            concurrently = "CONCURRENTLY " if cur.fetchone()[0] else ""
            start = time.time()
            cur.execute(f"REFRESH MATERIALIZED VIEW {concurrently}inpi.mv_patent_search")
            cur.execute("ANALYZE inpi.mv_patent_search")
            cur.execute("SELECT count(*) FROM inpi.mv_patent_search")
            logger.info(f"inpi.mv_patent_search: {cur.fetchone()[0]:,} patents, refreshed in {time.time() - start:.0f}s")
    finally:
        conn.close()
