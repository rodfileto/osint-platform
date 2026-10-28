"""Build silver tables: one output file per bronze file, rebuilt only when the bronze file is newer."""

import time
from pathlib import Path

import duckdb

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

SHORT_ID_REGEX = r"^https?://openalex\.org/([a-z-]+s/)?"


def short_id_sql(expr: str) -> str:
    return f"nullif(regexp_replace({expr}, '{SHORT_ID_REGEX}', ''), '')"


MACROS = f"CREATE OR REPLACE MACRO short_id(url) AS {short_id_sql('url')};"


def _quote(path: Path) -> str:
    return str(path).replace("'", "''")


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads = {threads}")
    con.execute(MACROS)
    return con


def sources(bronze: Path, entity: str) -> list[Path]:
    return sorted((bronze / entity).glob("*/*.parquet"))


def sample(files: list[Path], n: int) -> list[Path]:
    """Pick n files spread evenly across the list (covers old and recent partitions)."""
    if n >= len(files):
        return files
    step = len(files) / n
    return [files[int(i * step)] for i in range(n)]


def target(src: Path, bronze: Path, entity: str, out_dir: Path) -> Path:
    return out_dir / src.relative_to(bronze / entity)


def is_stale(src: Path, dest: Path) -> bool:
    return not dest.exists() or dest.stat().st_mtime < src.stat().st_mtime


def build_file(con: duckdb.DuckDBPyConnection, sql: str, src: Path, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    query = sql.strip().rstrip(";").replace("{src}", _quote(src))
    con.execute(f"COPY ({query}) TO '{_quote(tmp)}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(dest)
    return con.execute("SELECT count(*) FROM read_parquet(?)", [str(dest)]).fetchone()[0]


def prune(out_dir: Path, srcs: list[Path], bronze: Path, entity: str) -> int:
    keep = {target(s, bronze, entity, out_dir) for s in srcs}
    removed = 0
    for p in out_dir.rglob("*"):
        if p.is_file() and p not in keep:
            p.unlink()
            removed += 1
    return removed


def build_table(
    table: str,
    bronze: Path,
    silver: Path,
    limit: int | None = None,
    force: bool = False,
    threads: int | None = None,
    log=print,
) -> None:
    entity = TABLES[table]
    sql = (SQL_DIR / f"{table}.sql").read_text()
    out_dir = silver / table
    all_srcs = sources(bronze, entity)
    srcs = sample(all_srcs, limit) if limit else all_srcs
    if not limit:
        removed = prune(out_dir, all_srcs, bronze, entity)
        if removed:
            log(f"{table}: removed {removed} orphaned files")

    todo = [s for s in srcs if force or is_stale(s, target(s, bronze, entity, out_dir))]
    log(f"{table}: {len(srcs)} source files, {len(todo)} to build")
    con = connect(threads)
    start, rows = time.time(), 0
    for i, src in enumerate(todo, 1):
        rows += build_file(con, sql, src, target(src, bronze, entity, out_dir))
        log(f"[{i}/{len(todo)}] {rows:,} rows, {time.time() - start:.0f}s "
            f"{src.relative_to(bronze)}")
    log(f"{table}: done")
