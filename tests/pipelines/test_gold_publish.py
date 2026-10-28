"""The gold publish: gold Parquet → *_next tables → atomic swap into cnpj.

Needs a PostgreSQL with the Flyway migrations applied (all four schemas) and
POSTGRES_* env vars; skipped without them.
"""

import os
import shutil
import sys
from pathlib import Path

import duckdb
import psycopg2
import pytest

from gold_fixtures import LATEST, build_silver, make_storage, run_dbt

pytestmark = pytest.mark.skipif(
    shutil.which("dbt") is None or os.getenv("POSTGRES_HOST") is None,
    reason="dbt and a PostgreSQL (POSTGRES_* env) are required",
)

SCRIPTS = Path(__file__).resolve().parents[2] / "pipelines" / "scripts" / "cnpj"


def _pg_params():
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "osint_test"),
        "user": os.getenv("POSTGRES_USER", "osint_admin"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


@pytest.fixture
def pg():
    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cnpj.download_manifest (reference_month, file_name, file_type, processing_status, transformed_at) "
                "VALUES (%s, %s, 'socios', 'loaded', now()) "
                "ON CONFLICT (reference_month, file_name) DO NOTHING",
                (LATEST, f"Socios0-{LATEST}.zip"),
            )
            for table in ("empresa_old", "estabelecimento_old", "socio_old"):
                cur.execute(f"DROP TABLE IF EXISTS cnpj.{table}")
    finally:
        conn.close()
    yield
    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for table in ("empresa_old", "estabelecimento_old", "socio_old"):
                cur.execute(f"DROP TABLE IF EXISTS cnpj.{table}")
    finally:
        conn.close()


def _counts():
    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            out = {}
            for table in (
                "empresa", "estabelecimento", "socio", "simples_nacional",
                "socio_history", "company_search", "cnae", "municipio",
            ):
                cur.execute(f"SELECT count(*) FROM cnpj.{table}")
                out[table] = cur.fetchone()[0]
            return out
    finally:
        conn.close()


def test_gold_publish_builds_and_swaps(tmp_path, monkeypatch, pg):
    root = make_storage(tmp_path)

    con = duckdb.connect()
    try:
        build_silver(root, con)
    finally:
        con.close()

    result = run_dbt(tmp_path, root, "build", "--select", "cnpj_gold", reference_month=LATEST)
    assert result.returncode == 0, result.stdout + result.stderr

    monkeypatch.setenv("GOLD_STORAGE_ROOT", str(root))
    monkeypatch.setenv("CNPJ_TEMP_SSD_PATH", str(tmp_path / "temp_ssd"))
    sys.path.insert(0, str(SCRIPTS))
    import gold_publish

    monkeypatch.setattr(sys, "argv", ["gold_publish.py", LATEST])
    gold_publish.main()

    counts = _counts()
    assert counts == {
        "empresa": 1,
        "estabelecimento": 2,
        "socio": 3,
        "simples_nacional": 1,
        "socio_history": 5,
        "company_search": 1,
        "cnae": 2,
        "municipio": 2,
    }

    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # The publish marks the month as loaded, so select_month stops picking it.
            cur.execute(
                "SELECT count(*) FILTER (WHERE loaded_postgres_at IS NULL) FROM cnpj.download_manifest "
                "WHERE reference_month = %s AND transformed_at IS NOT NULL",
                (LATEST,),
            )
            assert cur.fetchone()[0] == 0

            # socio has no surrogate id anymore (Step 4).
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'cnpj' AND table_name = 'socio' AND column_name = 'id'"
            )
            assert cur.fetchall() == []

            # The canonical indexes and PK constraints landed on the live tables.
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'cnpj' "
                "AND indexname IN ('idx_company_search_cnpj_14', 'idx_company_search_razao_social_trgm_active', "
                "'idx_company_search_name_order_inactive', 'idx_socio_history_current', 'idx_socio_cpf_nome')"
            )
            assert {row[0] for row in cur.fetchall()} == {
                "idx_company_search_cnpj_14",
                "idx_company_search_razao_social_trgm_active",
                "idx_company_search_name_order_inactive",
                "idx_socio_history_current",
                "idx_socio_cpf_nome",
            }
            cur.execute(
                "SELECT conname FROM pg_constraint WHERE conname IN "
                "('empresa_pkey', 'estabelecimento_pkey')"
            )
            assert {row[0] for row in cur.fetchall()} == {
                "empresa_pkey", "estabelecimento_pkey",
            }

            # The dependent views were recreated inside the swap transaction.
            cur.execute("SELECT count(*) FROM cnpj.estabelecimento_completo")
            assert cur.fetchone()[0] == 2
            cur.execute("SELECT count(*) FROM geo.v_cnpj_br_municipality")
            assert cur.fetchone()[0] == 2

            # The previous live tables are kept as _old for rollback.
            cur.execute("SELECT to_regclass('cnpj.empresa_old') IS NOT NULL")
            assert cur.fetchone()[0] is True

            # A second publish for the same month is idempotent.
            gold_publish.main()
            assert _counts() == counts
            cur.execute("SELECT count(*) FROM cnpj.estabelecimento_completo")
            assert cur.fetchone()[0] == 2

            # The search materialized views are gone (Step 4); company_search is the source.
            cur.execute("SELECT to_regclass('cnpj.mv_company_search')")
            assert cur.fetchone()[0] is None
    finally:
        conn.close()


def test_gold_publish_swap_gives_up_on_a_busy_table(tmp_path, monkeypatch, pg):
    root = make_storage(tmp_path)

    con = duckdb.connect()
    try:
        build_silver(root, con)
    finally:
        con.close()

    result = run_dbt(tmp_path, root, "build", "--select", "cnpj_gold", reference_month=LATEST)
    assert result.returncode == 0, result.stdout + result.stderr

    monkeypatch.setenv("GOLD_STORAGE_ROOT", str(root))
    monkeypatch.setenv("CNPJ_TEMP_SSD_PATH", str(tmp_path / "temp_ssd"))
    sys.path.insert(0, str(SCRIPTS))
    import gold_publish

    monkeypatch.setattr(gold_publish, "SWAP_LOCK_TIMEOUT", "200ms")
    monkeypatch.setattr(gold_publish, "SWAP_ATTEMPTS", 2)
    monkeypatch.setattr(gold_publish, "SWAP_RETRY_SECONDS", 0)
    monkeypatch.setattr(sys, "argv", ["gold_publish.py", LATEST])
    before = _counts()

    # A long API read on empresa: the rename cannot get its ACCESS EXCLUSIVE lock.
    reader = psycopg2.connect(**_pg_params())
    try:
        with reader.cursor() as cur:
            cur.execute("SELECT count(*) FROM cnpj.empresa")
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                gold_publish.main()
        # The swap rolled back: the live tables are untouched and still readable.
        assert _counts() == before
    finally:
        reader.close()

    gold_publish.main()
    assert _counts()["socio_history"] == 5
