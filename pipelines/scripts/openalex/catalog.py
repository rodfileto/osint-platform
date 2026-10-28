"""Verify bronze against its manifest, and build the DuckDB catalog (lake.duckdb).

The catalog holds no data: one view per bronze entity (ids shortened to match silver) over MinIO,
one view per silver table over the NVMe, and lake_release with the release date. Queries on the
bronze views need the MinIO S3 secret in the caller's session; silver views are plain paths.
"""

import json
import os

from . import state, storage
from .config import Lake
from .storage import short_id_sql
from .sync import MANIFEST_FILE, manifest_files


def bronze_manifest(lake: Lake, s3) -> dict:
    from botocore.exceptions import ClientError

    try:
        body = s3.get_object(Bucket=lake.bucket, Key=lake.object_key(MANIFEST_FILE))["Body"].read()
    except ClientError:
        raise SystemExit(f"s3://{lake.bucket}/{lake.object_key(MANIFEST_FILE)} missing: "
                         "bronze is not a complete release (run sync)")
    return json.loads(body)


def verify(lake: Lake, log=print) -> bool:
    """Check every file's size and Parquet footer record count against the manifest."""
    s3 = storage.s3_client()
    manifest = bronze_manifest(lake, s3)
    files = manifest_files(manifest)
    sizes = storage.list_objects(s3, lake.bucket, lake.prefix)
    con = storage.duckdb_connect()
    bad = 0
    for i, f in enumerate(files, 1):
        if sizes.get(lake.object_key(f.rel_path)) != f.size:
            log(f"BAD size/missing: {f.rel_path}")
            bad += 1
            continue
        rows = con.execute(
            "SELECT sum(row_group_num_rows) FROM (SELECT DISTINCT row_group_id, row_group_num_rows "
            "FROM parquet_metadata(?))", [lake.s3_url(f.rel_path)]
        ).fetchone()[0]
        if rows != f.records:
            log(f"BAD record count {rows} != {f.records}: {f.rel_path}")
            bad += 1
        if i % 200 == 0:
            log(f"[{i}/{len(files)}] checked")
    con.close()
    log(f"verified {len(files)} files of release {manifest['date']}, {bad} problems")
    if bad == 0:
        conn = state.connect()
        try:
            state.mark_release(conn, manifest["date"], "verified_at")
        finally:
            conn.close()
    return bad == 0


def build_catalog(lake: Lake, log=print) -> None:
    s3 = storage.s3_client()
    manifest = bronze_manifest(lake, s3)
    entities = sorted({f.entity for f in manifest_files(manifest)})
    lake.home.mkdir(parents=True, exist_ok=True)
    tmp = lake.catalog.with_name(lake.catalog.name + ".tmp")
    tmp.unlink(missing_ok=True)
    # Built under another name and moved into place, so a reader attached to the old file keeps it.
    con = storage.duckdb_connect(path=str(tmp))
    for ent in entities:
        con.execute(
            f"CREATE OR REPLACE VIEW {ent.replace('-', '_')} AS "
            f"SELECT * REPLACE ({short_id_sql('id')} AS id) "
            f"FROM read_parquet('s3://{lake.bucket}/{lake.prefix}/{ent}/*/*.parquet', hive_partitioning = false)"
        )
    tables = sorted(p.name for p in lake.silver.iterdir() if p.is_dir()) if lake.silver.exists() else []
    for table in tables:
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('{lake.silver / table}/**/*.parquet', hive_partitioning = false)"
        )
    con.execute("CREATE OR REPLACE TABLE lake_release AS SELECT ?::DATE AS release_date", [manifest["date"]])
    # The secret was created for building only; consumers bring their own.
    con.execute("DROP SECRET IF EXISTS osint_minio")
    con.close()
    os.replace(tmp, lake.catalog)

    conn = state.connect()
    try:
        state.mark_release(conn, manifest["date"], "catalog_built_at")
    finally:
        conn.close()
    log(f"catalog {lake.catalog}: {len(entities)} bronze + {len(tables)} silver views, "
        f"release {manifest['date']}")
