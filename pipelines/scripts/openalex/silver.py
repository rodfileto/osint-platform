"""Build silver tables from bronze: one output file per bronze file, on the NVMe.

A silver file is rebuilt when the bronze file it came from has another source ETag than the one
recorded in openalex.silver_file (or the output is missing); silver files whose bronze file left
the release are removed.
"""

import time
from pathlib import Path

import duckdb

from . import state, storage
from .config import Lake

SQL_DIR = Path(__file__).parent / "sql"

# silver table -> bronze entity it is derived from
TABLES = {
    "works_core": "works",
    "work_references": "works",
    "work_authorships": "works",
    "work_topics": "works",
    "work_keywords": "works",
    "work_concepts": "works",
    "work_abstracts": "works",
}


def connect(lake: Lake | None = None, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    return storage.duckdb_connect(lake, threads)


def _quote(path) -> str:
    return str(path).replace("'", "''")


def sample(keys: list, n: int) -> list:
    """Pick n keys spread evenly across the list (covers old and recent partitions)."""
    if n >= len(keys):
        return keys
    step = len(keys) / n
    return [keys[int(i * step)] for i in range(n)]


def target(key: str, entity: str, out_dir: Path) -> Path:
    return out_dir / key.removeprefix(f"{entity}/")


def stale(sources: dict[str, tuple[int, str]], built: dict[str, str], out_dir: Path, entity: str,
          force: bool = False) -> list[str]:
    """Bronze keys whose silver file is missing or was built from another source ETag."""
    return [
        key for key, (_, etag) in sorted(sources.items())
        if force or built.get(key) != etag or not target(key, entity, out_dir).exists()
    ]


def build_file(con: duckdb.DuckDBPyConnection, sql: str, src: str, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    query = sql.strip().rstrip(";").replace("{src}", _quote(src))
    con.execute(f"COPY ({query}) TO '{_quote(tmp)}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(dest)
    return con.execute("SELECT count(*) FROM read_parquet(?)", [str(dest)]).fetchone()[0]


def prune(conn, table: str, out_dir: Path, sources: dict, built: dict, entity: str) -> int:
    keep = {target(key, entity, out_dir) for key in sources}
    removed = 0
    if out_dir.exists():
        for p in out_dir.rglob("*"):
            if p.is_file() and p not in keep:
                p.unlink()
                removed += 1
    gone = [key for key in built if key not in sources]
    if gone:
        state.forget_silver(conn, table, gone)
    return removed


def build_table(lake: Lake, table: str, limit: int | None = None, force: bool = False,
                threads: int | None = None, log=print) -> None:
    entity = TABLES[table]
    sql = (SQL_DIR / f"{table}.sql").read_text()
    out_dir = lake.silver / table
    conn = state.connect()
    try:
        sources = state.bronze_files(conn, entity)
        built = state.silver_files(conn, table)
        if not limit:
            removed = prune(conn, table, out_dir, sources, built, entity)
            if removed:
                log(f"{table}: removed {removed} silver files whose bronze file left the release")
        todo = stale(sources, built, out_dir, entity, force)
        if limit:
            todo = sample(todo, limit)
        log(f"{table}: {len(sources)} bronze files, {len(todo)} to build")
        con = connect(lake, threads)
        start, rows = time.time(), 0
        for i, key in enumerate(todo, 1):
            n = build_file(con, sql, lake.s3_url(key), target(key, entity, out_dir))
            state.record_silver(conn, table, key, sources[key][1], n)
            rows += n
            log(f"[{i}/{len(todo)}] {rows:,} rows, {time.time() - start:.0f}s {key}")
        con.close()
        log(f"{table}: done")
    finally:
        conn.close()
