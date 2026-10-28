"""The OpenAlex lake end to end: a fake upstream bucket → bronze in MinIO → silver → catalog.

Needs a MinIO (MINIO_ENDPOINT_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD) and a PostgreSQL with the
Flyway migrations (POSTGRES_*); skipped without them. scripts/run-tests.sh starts both.
"""

import json
import os
import uuid

import duckdb
import pytest

pytestmark = pytest.mark.skipif(
    not (os.getenv("POSTGRES_HOST") and os.getenv("MINIO_ENDPOINT_URL")),
    reason="needs MinIO (MINIO_*) and PostgreSQL (POSTGRES_*)",
)

WORKS = "works/updated_date=2026-01-01/part_0000.parquet"
KEYWORDS = "keywords/updated_date=2026-01-01/part_0000.parquet"


@pytest.fixture
def lake(tmp_path):
    from openalex import state, storage
    from openalex.config import Lake

    s3 = storage.s3_client()
    upstream, bronze = f"oa-upstream-{uuid.uuid4().hex[:8]}", f"oa-bronze-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=upstream)
    s3.put_bucket_policy(Bucket=upstream, Policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": ["*"]},
                       "Action": ["s3:GetObject", "s3:ListBucket"],
                       "Resource": [f"arn:aws:s3:::{upstream}", f"arn:aws:s3:::{upstream}/*"]}],
    }))
    conn = state.connect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE openalex.release, openalex.bronze_file, openalex.silver_file")
    conn.close()
    yield Lake(
        upstream_url=f"{os.environ['MINIO_ENDPOINT_URL'].rstrip('/')}/{upstream}",
        bucket=bronze,
        prefix="openalex/parquet",
        home=tmp_path / "lake",
        staging=tmp_path / "staging",
        duckdb_memory_limit="1GB",
        duckdb_temp=tmp_path / "duckdb_tmp",
    ), s3, upstream
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in {upstream, bronze} & existing:
        keys = [k for prefix in ("data", "openalex") for k in storage.list_objects(s3, bucket, prefix)]
        if keys:
            storage.delete_objects(s3, bucket, keys)
        s3.delete_bucket(Bucket=bucket)


def publish(s3, bucket, tmp_path, date, works_sql, keywords=True, manifest_extra=()):
    """Write a release into the fake upstream bucket the way OpenAlex lays it out."""
    con = duckdb.connect()
    files = {WORKS: works_sql}
    if keywords:
        files[KEYWORDS] = "SELECT 'https://openalex.org/keywords/xml' AS id, 'XML' AS display_name"
    entities = {}
    for rel, sql in files.items():
        local = tmp_path / "upstream" / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{local}' (FORMAT parquet)")
        records = con.execute(f"SELECT count(*) FROM '{local}'").fetchone()[0]
        s3.upload_file(str(local), bucket, f"data/parquet/{rel}")
        entities.setdefault(rel.split("/")[0], []).append({
            "url": f"s3://openalex/data/parquet/{rel}",
            "meta": {"content_length": local.stat().st_size, "record_count": records},
        })
    for rel in manifest_extra:
        entities.setdefault(rel.split("/")[0], []).append({
            "url": f"s3://openalex/data/parquet/{rel}", "meta": {"content_length": 1, "record_count": 1},
        })
    manifest = {"date": date, "entities": [{"entity": e, "files": f} for e, f in entities.items()]}
    s3.put_object(Bucket=bucket, Key="data/parquet/manifest.json", Body=json.dumps(manifest).encode())


WORKS_V1 = """SELECT * FROM (VALUES
    ('https://openalex.org/W2', 2020, ['https://openalex.org/W9', 'https://openalex.org/W1']),
    ('https://openalex.org/W1', 2019, NULL::VARCHAR[])
) t(id, publication_year, referenced_works)"""
WORKS_V2 = """SELECT * FROM (VALUES
    ('https://openalex.org/W2', 2020, ['https://openalex.org/W9', 'https://openalex.org/W1', 'https://openalex.org/W3'])
) t(id, publication_year, referenced_works)"""


def last(logs, text):
    """The last log line that mentions text."""
    return next(line for line in reversed(logs) if text in line)


def test_sync_verify_silver_catalog_and_a_new_release(lake, tmp_path):
    from openalex import catalog, silver, state, storage, sync

    lake, s3, upstream = lake
    logs = []
    publish(s3, upstream, tmp_path, "2026-10-14", WORKS_V1)

    assert sync.sync(lake, workers=2, log=logs.append)
    objects = storage.list_objects(s3, lake.bucket, lake.prefix)
    assert set(objects) == {lake.object_key(WORKS), lake.object_key(KEYWORDS), lake.object_key("manifest.json")}
    assert not any(lake.staging.rglob("*.parquet")), "staging is emptied after each upload"

    assert sync.sync(lake, log=logs.append)
    assert "2 files, 0 to download" in last(logs, "to download"), "an unchanged release downloads nothing"

    assert catalog.verify(lake, log=logs.append)
    silver.build_table(lake, "work_references", log=logs.append)
    out = lake.silver / "work_references" / WORKS.removeprefix("works/")
    assert duckdb.connect().execute(f"SELECT citing, cited FROM '{out}' ORDER BY 1, 2").fetchall() == [
        ("W2", "W1"), ("W2", "W9"),
    ]
    silver.build_table(lake, "work_references", log=logs.append)
    assert "1 bronze files, 0 to build" in last(logs, "to build")

    catalog.build_catalog(lake, log=logs.append)
    con = storage.duckdb_connect()  # brings the MinIO secret, as a consumer would
    con.execute(f"ATTACH '{lake.catalog}' AS lake (READ_ONLY)")
    assert con.execute("SELECT release_date::VARCHAR FROM lake.lake_release").fetchone() == ("2026-10-14",)
    assert con.execute("SELECT id, display_name FROM lake.keywords").fetchall() == [("xml", "XML")]
    assert con.execute("SELECT count(*) FROM lake.work_references").fetchone() == (2,)
    con.close()

    conn = state.connect()
    release = state.latest_release(conn)
    assert all(release[m] for m in ("synced_at", "verified_at", "catalog_built_at"))

    # Next quarter: works changes, keywords disappears. Only works is downloaded, keywords is
    # removed from bronze, and the one silver file is rebuilt from the new version.
    publish(s3, upstream, tmp_path, "2027-01-13", WORKS_V2, keywords=False)
    s3.delete_object(Bucket=upstream, Key=f"data/parquet/{KEYWORDS}")
    assert sync.sync(lake, log=logs.append)
    assert "1 files, 1 to download" in next(line for line in logs if "2027-01-13" in line)
    assert set(storage.list_objects(s3, lake.bucket, lake.prefix)) == {
        lake.object_key(WORKS), lake.object_key("manifest.json"),
    }
    assert set(state.bronze_files(conn)) == {WORKS}
    silver.build_table(lake, "work_references", log=logs.append)
    assert "1 bronze files, 1 to build" in last(logs, "to build")
    assert duckdb.connect().execute(f"SELECT count(*) FROM '{out}'").fetchone() == (3,)
    conn.close()


def test_sync_waits_while_the_bucket_is_being_replaced(lake, tmp_path):
    from openalex import sync

    lake, s3, upstream = lake
    logs = []
    # The manifest already lists a file that is not uploaded yet.
    publish(s3, upstream, tmp_path, "2026-10-14", WORKS_V1, manifest_extra=["works/updated_date=2026-01-02/part_0000.parquet"])

    assert not sync.sync(lake, log=logs.append)
    assert "not listed upstream" in logs[-1]
    buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    assert lake.bucket not in buckets, "a partial release is not copied at all"
