import json
from pathlib import Path

import duckdb

from .silver import short_id_sql
from .sync import MANIFEST_FILE, manifest_files


def local_manifest(bronze: Path) -> dict:
    p = bronze / MANIFEST_FILE
    if not p.exists():
        raise SystemExit(f"{p} missing: bronze is not a complete release (run `lake sync`)")
    return json.loads(p.read_text())


def verify(bronze: Path, log=print) -> bool:
    """Check every file's size and parquet footer record count against the manifest."""
    files = manifest_files(local_manifest(bronze))
    con = duckdb.connect()
    bad = 0
    for f in files:
        p = bronze / f.rel_path
        if not p.exists() or p.stat().st_size != f.size:
            log(f"BAD size/missing: {f.rel_path}")
            bad += 1
            continue
        rows = con.execute(
            "SELECT sum(row_group_num_rows) FROM (SELECT DISTINCT row_group_id, row_group_num_rows "
            "FROM parquet_metadata(?))", [str(p)]
        ).fetchone()[0]
        if rows != f.records:
            log(f"BAD record count {rows} != {f.records}: {f.rel_path}")
            bad += 1
    log(f"verified {len(files)} files, {bad} problems")
    return bad == 0


def build_catalog(bronze: Path, silver: Path, catalog: Path, log=print) -> None:
    manifest = local_manifest(bronze)
    entities = sorted({f.rel_path.split("/", 1)[0] for f in manifest_files(manifest)})
    catalog.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(catalog))
    for ent in entities:
        view = ent.replace("-", "_")
        glob = str(bronze / ent / "*" / "*.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * REPLACE ({short_id_sql('id')} AS id) "
            f"FROM read_parquet('{glob}', hive_partitioning = false)"
        )
    tables = sorted(p.name for p in silver.iterdir() if p.is_dir()) if silver.exists() else []
    for table in tables:
        glob = str(silver / table / "**" / "*.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning = false)"
        )
    con.execute("CREATE OR REPLACE TABLE lake_release AS SELECT ?::DATE AS release_date",
                [manifest["date"]])
    con.close()
    log(f"catalog {catalog}: {len(entities)} bronze + {len(tables)} silver views, "
        f"release {manifest['date']}")
