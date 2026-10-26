import sys
from pathlib import Path

import duckdb
import pytest

PIPELINES = Path(__file__).resolve().parents[2] / "pipelines"

# The same import roots Airflow uses: DAG task modules, CNPJ scripts and the scripts package.
for path in (PIPELINES / "dags", PIPELINES / "scripts" / "cnpj", PIPELINES / "scripts"):
    sys.path.insert(0, str(path))


@pytest.fixture
def run_query(tmp_path):
    """Write `lines` as a UTF-8 CSV, run a cleaner query builder on it, return the Parquet rows."""

    def run(builder, lines, *args):
        csv_path, parquet_path = tmp_path / "input.csv", tmp_path / "output.parquet"
        csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        con = duckdb.connect()
        try:
            con.execute(builder(str(csv_path), str(parquet_path), *args))
            cursor = con.execute(f"SELECT * FROM '{parquet_path}'")
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            con.close()

    return run
