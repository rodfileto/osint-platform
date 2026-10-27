"""The dbt gold layer: silver Parquet → gold Parquet models and tests.

Builds a three-month silver fixture with the real cleaners, runs `dbt build`
against it (models + data tests), then checks the gold Parquet contents.
"""

import shutil
from datetime import date

import duckdb
import pytest

from gold_fixtures import LATEST, build_silver, make_storage, run_dbt

pytestmark = pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt not installed")


def test_dbt_gold_build_and_tests(tmp_path):
    root = make_storage(tmp_path)

    con = duckdb.connect()
    try:
        build_silver(root, con)
    finally:
        con.close()

    result = run_dbt(tmp_path, root, "build", "--select", "cnpj_gold", reference_month=LATEST)
    assert result.returncode == 0, result.stdout + result.stderr

    verify = duckdb.connect()
    try:
        def rows(table):
            return verify.execute(
                f"SELECT * FROM '{root / 'cnpj' / 'gold' / table / f'{table}.parquet'}'"
            ).fetchall()

        empresa = rows("empresa")
        assert len(empresa) == 1
        assert empresa[0][0] == "01234567"
        assert empresa[0][1] == "ACME LTDA", "the most complete duplicate wins"

        assert len(rows("estabelecimento")) == 2
        assert len(rows("socio")) == 3
        assert len(rows("simples_nacional")) == 1

        history = rows("socio_history")
        assert len(history) == 5

        maria = [r for r in history if r[3] == "MARIA DA SILVA"]
        assert len(maria) == 2, "maria left and came back: two periods"
        holding = [r for r in history if r[3] == "HOLDING SA"]
        assert len(holding) == 1
        assert holding[0][5] == date(2026, 8, 1) and holding[0][6] == date(2026, 10, 1)
        assert holding[0][7] is True
        joao = [r for r in history if r[3] == "JOAO PEREIRA"]
        assert joao[0][5] == date(2026, 8, 1) and joao[0][6] == date(2026, 8, 1)
        assert joao[0][7] is False

        current = [r for r in history if r[7]]
        assert len(current) == len(rows("socio")), "history invariant: current periods = latest socio rows"

        search = rows("company_search")
        assert len(search) == 1
        assert search[0][5] == "01234567000111"
        assert search[0][19] is True
        assert search[0][8] == "Guaíba"
    finally:
        verify.close()


def test_dbt_gold_auto_month_picks_latest(tmp_path):
    root = make_storage(tmp_path)

    con = duckdb.connect()
    try:
        build_silver(root, con)
    finally:
        con.close()

    result = run_dbt(tmp_path, root, "run", "--select", "empresa")
    assert result.returncode == 0, result.stdout + result.stderr

    verify = duckdb.connect()
    try:
        rows = verify.execute(
            f"SELECT * FROM '{root / 'cnpj' / 'gold' / 'empresa' / 'empresa.parquet'}'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "01234567"
    finally:
        verify.close()


def test_socio_history_ignores_months_missing_from_silver(tmp_path):
    root = make_storage(tmp_path)

    con = duckdb.connect()
    try:
        build_silver(root, con)
    finally:
        con.close()
    # 2026-09 was never downloaded: drop it from every silver entity.
    for partition in (root / "cnpj" / "silver").glob("*/reference_month=2026-09"):
        shutil.rmtree(partition)

    result = run_dbt(tmp_path, root, "build", "--select", "cnpj_gold", reference_month=LATEST)
    assert result.returncode == 0, result.stdout + result.stderr

    verify = duckdb.connect()
    try:
        history = verify.execute(
            f"SELECT nome_socio_razao_social, valid_from, valid_to, is_current "
            f"FROM '{root / 'cnpj' / 'gold' / 'socio_history' / 'socio_history.parquet'}'"
        ).fetchall()
    finally:
        verify.close()

    # Seen in 2026-08 and 2026-10, the only months in silver: one period, not two.
    maria = [r for r in history if r[0] == "MARIA DA SILVA"]
    assert maria == [("MARIA DA SILVA", date(2026, 8, 1), date(2026, 10, 1), True)]
    holding = [r for r in history if r[0] == "HOLDING SA"]
    assert holding == [("HOLDING SA", date(2026, 8, 1), date(2026, 10, 1), True)]
