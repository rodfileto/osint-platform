"""CNPJ gold publish: gold Parquet in MinIO → PostgreSQL serving tables.

For each gold table:
1. `DROP TABLE IF EXISTS cnpj.<t>_old` (the previous run's rollback copy).
2. `CREATE TABLE cnpj.<t>_next (LIKE cnpj.<t> INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`.
3. COPY the gold Parquet into it through DuckDB's postgres extension.
4. ANALYZE and build the PK constraint and indexes with `_next`-suffixed names.

Then, in ONE transaction (metadata-only, so it holds locks for milliseconds once it has them):
5. Drop the live table's canonical indexes and PK constraint (transactional).
6. Rename live → `_old`, `_next` → live.
7. Rename the `_next` indexes and PK constraint to their canonical names.
8. Recreate the dependent views from serving_views.sql.
9. COMMIT.

`_old` is kept until the next run for rollback; the real rollback is re-publishing.

The renames need ACCESS EXCLUSIVE locks. Waiting behind a long API query would queue
every new query on that table behind the swap, so the swap gives up after a short
lock_timeout, rolls back and retries; the _next tables are kept between attempts.
"""

import os
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
import psycopg2
from psycopg2 import errors

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gold_guard

TABLES = [
    {
        "name": "cnae",
        "columns": "cnae_fiscal, descricao",
        "pk": ("cnae_pkey", "cnae_fiscal"),
        "indexes": [],
    },
    {
        "name": "motivo_situacao_cadastral",
        "columns": "codigo, descricao",
        "pk": ("motivo_situacao_cadastral_pkey", "codigo"),
        "indexes": [],
    },
    {
        "name": "municipio",
        "columns": "codigo, nome",
        "pk": ("municipio_pkey", "codigo"),
        "indexes": [],
    },
    {
        "name": "natureza_juridica",
        "columns": "codigo, descricao",
        "pk": ("natureza_juridica_pkey", "codigo"),
        "indexes": [],
    },
    {
        "name": "pais",
        "columns": "codigo, nome",
        "pk": ("pais_pkey", "codigo"),
        "indexes": [],
    },
    {
        "name": "qualificacao_socio",
        "columns": "codigo, descricao",
        "pk": ("qualificacao_socio_pkey", "codigo"),
        "indexes": [],
    },
    {
        "name": "empresa",
        "columns": (
            "cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, "
            "capital_social, porte_empresa, ente_federativo_responsavel, reference_month"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": ("empresa_pkey", "cnpj_basico"),
        "indexes": [
            ("idx_empresa_razao_social", "USING GIN (to_tsvector('portuguese', razao_social))"),
            ("idx_empresa_porte", "(porte_empresa)"),
            ("idx_empresa_natureza", "(natureza_juridica)"),
            ("idx_empresa_ref_month", "(reference_month)"),
        ],
    },
    {
        "name": "estabelecimento",
        "columns": (
            "cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, "
            "situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, "
            "nome_cidade_exterior, codigo_pais, data_inicio_atividade, cnae_fiscal_principal, "
            "cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento, bairro, "
            "cep, uf, codigo_municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, "
            "correio_eletronico, situacao_especial, data_situacao_especial, reference_month"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": ("estabelecimento_pkey", "cnpj_basico, cnpj_ordem, cnpj_dv"),
        "indexes": [
            ("idx_estabelecimento_nome_fantasia", "USING GIN (to_tsvector('portuguese', nome_fantasia))"),
            ("idx_estabelecimento_situacao", "(situacao_cadastral)"),
            ("idx_estabelecimento_municipio", "(codigo_municipio)"),
            ("idx_estabelecimento_uf", "(uf)"),
            ("idx_estabelecimento_cnae", "(cnae_fiscal_principal)"),
            ("idx_estabelecimento_ref_month", "(reference_month)"),
        ],
    },
    {
        "name": "simples_nacional",
        "columns": (
            "cnpj_basico, optante_simples_nacional, data_optante_simples_nacional, "
            "data_exclusao_simples_nacional, opcao_mei, data_opcao_mei, "
            "data_exclusao_mei, reference_month"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": ("simples_nacional_pkey", "cnpj_basico"),
        "indexes": [],
    },
    {
        "name": "socio",
        "columns": (
            "cnpj_basico, identificador_socio, nome_socio_razao_social, cpf_cnpj_socio, "
            "qualificacao_socio, data_entrada_sociedade, pais, representante_legal, "
            "nome_do_representante, qualificacao_representante_legal, faixa_etaria, "
            "reference_month"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": None,
        "indexes": [
            ("idx_socio_cnpj_basico", "(cnpj_basico)"),
            ("idx_socio_cpf_nome", "(cpf_cnpj_socio, nome_socio_razao_social)"),
            ("idx_socio_pj_cnpj_basico", "(left(cpf_cnpj_socio, 8)) WHERE identificador_socio = 1"),
            ("idx_socio_nome", "USING GIN (to_tsvector('portuguese', nome_socio_razao_social)) WHERE nome_socio_razao_social IS NOT NULL"),
            ("idx_socio_ref_month", "(reference_month)"),
            ("idx_socio_qualificacao", "(qualificacao_socio)"),
        ],
    },
    {
        "name": "socio_history",
        "columns": (
            "cnpj_basico, identificador_socio, cpf_cnpj_socio, nome_socio_razao_social, "
            "qualificacao_socio, valid_from, valid_to, is_current, data_entrada_sociedade, "
            "faixa_etaria, pais, representante_legal, nome_do_representante, "
            "qualificacao_representante_legal"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": None,
        "indexes": [
            ("idx_socio_history_cnpj_basico", "(cnpj_basico)"),
            ("idx_socio_history_cpf", "(cpf_cnpj_socio)"),
            ("idx_socio_history_current", "(cnpj_basico) WHERE is_current"),
        ],
    },
    {
        "name": "company_search",
        "columns": (
            "cnpj_basico, razao_social, nome_fantasia, cnpj_ordem, cnpj_dv, cnpj_14, "
            "situacao_cadastral, codigo_municipio, municipio_nome, uf, "
            "cnae_fiscal_principal, cnae_descricao, porte_empresa, natureza_juridica, "
            "natureza_juridica_descricao, capital_social, data_inicio_atividade, "
            "correio_eletronico, reference_month, is_active"
        ),
        "extras": ("created_at, updated_at", "now() AS created_at, now() AS updated_at"),
        "pk": None,
        "indexes": [
            ("idx_company_search_cnpj_14", "(cnpj_14)", "UNIQUE"),
            ("idx_company_search_cnpj_basico", "(cnpj_basico varchar_pattern_ops)"),
            ("idx_company_search_cnpj_14_pattern", "(cnpj_14 text_pattern_ops)"),
            ("idx_company_search_razao_social_trgm", "USING GIN (razao_social gin_trgm_ops)"),
            ("idx_company_search_nome_fantasia_trgm", "USING GIN (nome_fantasia gin_trgm_ops)"),
            ("idx_company_search_uf_municipio", "(uf, codigo_municipio)"),
            ("idx_company_search_situacao", "(situacao_cadastral)"),
        ],
    },
]

SERVING_VIEWS_SQL = Path(__file__).parent / "serving_views.sql"

TEMP_SSD_PATH = Path(os.getenv("CNPJ_TEMP_SSD_PATH", "/opt/airflow/temp_ssd"))

SWAP_LOCK_TIMEOUT = os.getenv("GOLD_SWAP_LOCK_TIMEOUT", "5s")
SWAP_ATTEMPTS = int(os.getenv("GOLD_SWAP_ATTEMPTS", "10"))
SWAP_RETRY_SECONDS = float(os.getenv("GOLD_SWAP_RETRY_SECONDS", "30"))


def _pg_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "osint_metadata"),
        "user": os.getenv("POSTGRES_USER", "osint_admin"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


@contextmanager
def _phase(description: str, heartbeat_seconds: int = 120):
    """Log when a long step starts, periodically while it runs, and when it ends."""
    start = time.time()
    finished = threading.Event()

    def heartbeat():
        while not finished.wait(heartbeat_seconds):
            logger = print
            logger(f"  {description}: still running ({time.time() - start:.0f}s)")

    print(f"  {description}...")
    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        yield
    finally:
        finished.set()
    print(f"  {description}: done in {time.time() - start:.1f}s")


def _gold_storage_root() -> str | None:
    """Local root override for tests/CI; None means the MinIO gold bucket."""
    root = os.getenv("GOLD_STORAGE_ROOT", "").rstrip("/")
    return root or None


def _gold_parquet_path(table: str) -> str:
    root = _gold_storage_root()
    if root:
        return f"{root}/cnpj/gold/{table}/{table}.parquet"
    import downloader

    return f"s3://{downloader.processed_bucket()}/cnpj/gold/{table}/{table}.parquet"


def _duckdb_secret_sql() -> str:
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    endpoint = endpoint_url.replace("https://", "").replace("http://", "")
    use_ssl = "false" if endpoint_url.startswith("http://") else "true"
    user = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    password = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))
    if not user or not password:
        raise RuntimeError("MinIO credentials not set (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)")
    return (
        f"CREATE OR REPLACE SECRET osint_minio (TYPE S3, KEY_ID '{user}', SECRET '{password}', "
        f"ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL {use_ssl});"
    )


def _check_gold_parquets() -> None:
    root = _gold_storage_root()
    if root:
        for spec in TABLES:
            path = _gold_parquet_path(spec["name"])
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Gold Parquet missing: {path}")
        print(f"All {len(TABLES)} gold Parquet files found under {root}/cnpj/gold/")
        return

    import downloader
    from botocore.exceptions import ClientError

    s3 = downloader.s3_client()
    bucket = downloader.processed_bucket()
    for spec in TABLES:
        key = f"cnpj/gold/{spec['name']}/{spec['name']}.parquet"
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except ClientError:
            raise FileNotFoundError(f"Gold Parquet missing: s3://{bucket}/{key}")
    print(f"All {len(TABLES)} gold Parquet files found in s3://{bucket}/cnpj/gold/")


def _copy_gold_to_next() -> None:
    spill_dir = TEMP_SSD_PATH / "duckdb_tmp"
    spill_dir.mkdir(parents=True, exist_ok=True)
    duck = duckdb.connect(config={"temp_directory": str(spill_dir), "memory_limit": "4GB"})
    try:
        if _gold_storage_root() is None:
            duck.execute("INSTALL httpfs; LOAD httpfs;")
            duck.execute(_duckdb_secret_sql())
        duck.execute("INSTALL postgres; LOAD postgres; SET pg_null_byte_replacement=''")
        dsn = " ".join(f"{k}={v}" for k, v in _pg_params().items())
        duck.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES)")
        for spec in TABLES:
            table, columns = spec["name"], spec["columns"]
            extras = spec.get("extras")
            insert_columns = f"{columns}, {extras[0]}" if extras else columns
            select_columns = f"{columns}, {extras[1]}" if extras else columns
            path = _gold_parquet_path(table)
            with _phase(f"COPYing {table} from {path}"):
                duck.execute(
                    f"INSERT INTO pg.cnpj.{table}_next ({insert_columns}) "
                    f"SELECT {select_columns} FROM '{path}'"
                )
            rows = duck.execute(f"SELECT count(*) FROM pg.cnpj.{table}_next").fetchone()[0]
            print(f"  cnpj.{table}_next: {rows:,} rows")
    finally:
        duck.close()


def _build_next_tables(conn) -> None:
    with conn.cursor() as cur:
        for spec in TABLES:
            table = spec["name"]
            with _phase(f"preparing cnpj.{table}_next"):
                cur.execute(f"DROP TABLE IF EXISTS cnpj.{table}_old CASCADE")
                cur.execute(f"DROP TABLE IF EXISTS cnpj.{table}_next")
                cur.execute(
                    f"CREATE TABLE cnpj.{table}_next "
                    f"(LIKE cnpj.{table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
                )

    _copy_gold_to_next()

    with conn.cursor() as cur:
        for spec in TABLES:
            table = spec["name"]
            with _phase(f"indexing cnpj.{table}_next"):
                cur.execute(f"ANALYZE cnpj.{table}_next")
                if spec["pk"]:
                    pk_name, pk_columns = spec["pk"]
                    cur.execute(
                        f"ALTER TABLE cnpj.{table}_next "
                        f"ADD CONSTRAINT {pk_name}_next PRIMARY KEY ({pk_columns})"
                    )
                for index in spec["indexes"]:
                    name, definition = index[0], index[1]
                    unique = "UNIQUE " if len(index) > 2 and index[2] == "UNIQUE" else ""
                    # A leftover _next-named index on the live table (interrupted
                    # earlier publish) would collide with this name; drop it first.
                    cur.execute(f"DROP INDEX IF EXISTS cnpj.{name}_next")
                    cur.execute(
                        f"CREATE {unique}INDEX {name}_next ON cnpj.{table}_next {definition}"
                    )


def _swap(conn) -> None:
    """Swap every live table with its _next copy, retrying when the locks are not free."""
    for attempt in range(1, SWAP_ATTEMPTS + 1):
        try:
            _swap_once(conn)
            return
        except errors.LockNotAvailable:
            if attempt == SWAP_ATTEMPTS:
                raise
            print(
                f"  swap attempt {attempt}/{SWAP_ATTEMPTS}: tables busy (lock_timeout {SWAP_LOCK_TIMEOUT}); "
                f"retrying in {SWAP_RETRY_SECONDS:.0f}s"
            )
            time.sleep(SWAP_RETRY_SECONDS)


def _swap_once(conn) -> None:
    """Swap every live table with its _next copy, in one transaction."""
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            with _phase("swapping tables (one transaction)"):
                cur.execute("SET LOCAL lock_timeout = %s", (SWAP_LOCK_TIMEOUT,))
                for spec in TABLES:
                    table = spec["name"]
                    if spec["pk"]:
                        cur.execute(f"ALTER TABLE cnpj.{table} DROP CONSTRAINT IF EXISTS {spec['pk'][0]}")
                    for index in spec["indexes"]:
                        cur.execute(f"DROP INDEX IF EXISTS cnpj.{index[0]}")
                for spec in TABLES:
                    table = spec["name"]
                    cur.execute(f"ALTER TABLE cnpj.{table} RENAME TO {table}_old")
                    cur.execute(f"ALTER TABLE cnpj.{table}_next RENAME TO {table}")
                for spec in TABLES:
                    table = spec["name"]
                    if spec["pk"]:
                        cur.execute(
                            f"ALTER TABLE cnpj.{table} RENAME CONSTRAINT {spec['pk'][0]}_next "
                            f"TO {spec['pk'][0]}"
                        )
                    for index in spec["indexes"]:
                        cur.execute(f"ALTER INDEX cnpj.{index[0]}_next RENAME TO {index[0]}")
                cur.execute(SERVING_VIEWS_SQL.read_text())
        conn.commit()
        print("  swap committed")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True


def _mark_published(conn, month: str) -> None:
    """Mark the month's transformed files as loaded, so select_month stops picking it."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE cnpj.download_manifest SET loaded_postgres_at = now(), updated_at = now() "
            "WHERE reference_month = %s AND transformed_at IS NOT NULL",
            (month,),
        )
        print(f"  marked {cur.rowcount} manifest file(s) of {month} as published")


def _counts(conn) -> None:
    with conn.cursor() as cur:
        for spec in TABLES:
            table = spec["name"]
            cur.execute(f"SELECT count(*), pg_size_pretty(pg_total_relation_size('cnpj.{table}')) FROM cnpj.{table}")
            rows, size = cur.fetchone()
            print(f"cnpj.{table}: {rows:,} rows, {size}")


def main() -> None:
    month = sys.argv[1] if len(sys.argv) > 1 else ""

    if not month:
        print("No reference month selected; skipping the gold publish")
        sys.exit(2)

    latest = gold_guard.latest_silver_month()
    if latest is None or latest > month:
        reason = "no silver month in the manifest" if latest is None else f"latest silver is {latest}"
        print(f"Selected month {month} is not publishable ({reason}); skipping the gold publish")
        sys.exit(2)

    _check_gold_parquets()

    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        _build_next_tables(conn)
        _swap(conn)
        _mark_published(conn, month)
        _counts(conn)
    finally:
        conn.close()

    print(f"Published gold for {month}")


if __name__ == "__main__":
    main()
