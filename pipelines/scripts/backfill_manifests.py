"""Rebuild download manifests from bronze data that already exists.

Sources:
  cnpj   s3://osint-raw/cnpj/raw/<YYYY-MM>/<File>.zip        -> cnpj.download_manifest
  inpi   s3://osint-raw/inpi/patentes/<YYYYMMDD>/<FILE>.csv  -> inpi.download_manifest
  finep  <FINEP_DIR>/<dataset>/<YYYYMMDD_HHMMSS>_<File>.xlsx -> finep.download_manifest

Never touches bronze: only lists objects/files and writes manifest rows.

Run inside the Airflow container:
  python /opt/airflow/scripts/backfill_manifests.py --dry-run
  python /opt/airflow/scripts/backfill_manifests.py --source cnpj --source inpi
  python /opt/airflow/scripts/backfill_manifests.py --replace
"""

import argparse
import hashlib
import importlib.util
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

SCRIPTS_DIR = Path(__file__).resolve().parent


def _load(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FINEP_DATASETS = _load("finep_downloader", "finep/downloader.py").FINEP_DATASETS
INPI_DATASETS = _load("inpi_downloader", "inpi/downloader.py").INPI_DATASETS
get_file_type = _load("cnpj_populate_manifest", "cnpj/populate_manifest.py").get_file_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_manifests")

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
CNPJ_PREFIX = os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw").rstrip("/")
INPI_PREFIX = os.getenv("MINIO_PREFIX_INPI", "inpi/patentes").rstrip("/")
FINEP_DIR = Path(os.getenv("FINEP_OUTPUT_DIR", "/opt/airflow/data/finep"))


def db_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def s3_client():
    import boto3
    from botocore.client import Config

    return boto3.session.Session().client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY")),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY")),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def list_objects(prefix: str) -> list[dict]:
    client = s3_client()
    try:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=RAW_BUCKET, Prefix=prefix + "/")
        return [obj for page in pages for obj in page.get("Contents", []) or []]
    except client.exceptions.NoSuchBucket:
        raise SystemExit(
            f"Bucket '{RAW_BUCKET}' not found at {client.meta.endpoint_url}. "
            "Is MinIO's data dir pointing at the disk that holds bronze?"
        )


# --------------------------------------------------------------------------- CNPJ

def scan_cnpj() -> list[dict]:
    rows = []
    for obj in list_objects(CNPJ_PREFIX):
        parts = obj["Key"][len(CNPJ_PREFIX) + 1:].split("/")
        if len(parts) != 2 or not re.fullmatch(r"\d{4}-\d{2}", parts[0]) or not parts[1].lower().endswith(".zip"):
            continue
        month, name = parts
        rows.append({
            "reference_month": month,
            "file_name": name,
            "file_type": get_file_type(name),
            "file_size_bytes": obj["Size"],
            "download_date": obj["LastModified"].replace(tzinfo=None),
        })
    return rows


def write_cnpj(cur, rows: list[dict], replace: bool) -> None:
    if replace:
        cur.execute("DELETE FROM cnpj.download_manifest")
    execute_values(cur, """
        INSERT INTO cnpj.download_manifest
            (reference_month, file_name, file_type, file_size_bytes, download_date, processing_status)
        VALUES %s
        ON CONFLICT (reference_month, file_name) DO UPDATE SET
            file_type       = EXCLUDED.file_type,
            file_size_bytes = EXCLUDED.file_size_bytes,
            download_date   = EXCLUDED.download_date,
            updated_at      = CURRENT_TIMESTAMP
    """, [(r["reference_month"], r["file_name"], r["file_type"], r["file_size_bytes"],
           r["download_date"], "downloaded") for r in rows])


# --------------------------------------------------------------------------- INPI

def scan_inpi() -> list[dict]:
    by_file = {Path(url).name: name for name, url in INPI_DATASETS.items()}
    rows = []
    for obj in list_objects(INPI_PREFIX):
        parts = obj["Key"][len(INPI_PREFIX) + 1:].split("/")
        if len(parts) != 2 or not re.fullmatch(r"\d{8}", parts[0]) or parts[1] not in by_file:
            continue
        snapshot, name = parts
        dataset = by_file[name]
        rows.append({
            "file_name": name,
            "source_url": INPI_DATASETS[dataset],
            "dataset_name": dataset,
            "minio_key": obj["Key"],
            "snapshot_date": datetime.strptime(snapshot, "%Y%m%d").date(),
            "download_date": obj["LastModified"],
            "file_size_bytes": obj["Size"],
        })

    # Loader and downloader only consider 'downloaded'; older snapshots stay listed but inert.
    latest = {}
    for r in rows:
        if r["snapshot_date"] > latest.get(r["dataset_name"], r["snapshot_date"].min):
            latest[r["dataset_name"]] = r["snapshot_date"]
    for r in rows:
        r["processing_status"] = "downloaded" if r["snapshot_date"] == latest[r["dataset_name"]] else "superseded"
    return rows


def write_inpi(cur, rows: list[dict], replace: bool) -> None:
    if replace:
        cur.execute("DELETE FROM inpi.download_manifest")
    execute_values(cur, """
        INSERT INTO inpi.download_manifest
            (file_name, source_url, dataset_name, minio_key, minio_bucket, snapshot_date,
             download_date, file_size_bytes, is_valid, processing_status)
        VALUES %s
        ON CONFLICT (minio_key) DO UPDATE SET
            file_size_bytes   = EXCLUDED.file_size_bytes,
            download_date     = EXCLUDED.download_date,
            processing_status = EXCLUDED.processing_status,
            updated_at        = CURRENT_TIMESTAMP
    """, [(r["file_name"], r["source_url"], r["dataset_name"], r["minio_key"], RAW_BUCKET,
           r["snapshot_date"], r["download_date"], r["file_size_bytes"], True,
           r["processing_status"]) for r in rows])


# --------------------------------------------------------------------------- FINEP

def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_finep() -> list[dict]:
    rows = []
    for dataset, url in FINEP_DATASETS.items():
        for path in sorted((FINEP_DIR / dataset).glob("*.xlsx")):
            match = re.match(r"^(\d{8}_\d{6})_", path.name)
            if match:
                downloaded = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            else:
                downloaded = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(tzinfo=None)
            rows.append({
                "file_name": path.name,
                "source_url": url,
                "dataset_type": dataset,
                "download_date": downloaded,
                "file_size_bytes": path.stat().st_size,
                "file_checksum_sha256": sha256(path),
                "local_path": str(path),
            })
    return rows


def write_finep(cur, rows: list[dict], replace: bool) -> None:
    # finep.download_manifest has no unique key; local_path identifies a file.
    if replace:
        cur.execute("DELETE FROM finep.download_manifest")
    else:
        cur.execute("DELETE FROM finep.download_manifest WHERE local_path = ANY(%s)",
                    ([r["local_path"] for r in rows],))
    execute_values(cur, """
        INSERT INTO finep.download_manifest
            (file_name, source_url, dataset_type, download_date, file_size_bytes,
             file_checksum_sha256, local_path, processing_status)
        VALUES %s
    """, [(r["file_name"], r["source_url"], r["dataset_type"], r["download_date"],
           r["file_size_bytes"], r["file_checksum_sha256"], r["local_path"], "downloaded") for r in rows])


# --------------------------------------------------------------------------- main

SOURCES = {
    "cnpj": (scan_cnpj, write_cnpj, "reference_month"),
    "inpi": (scan_inpi, write_inpi, "snapshot_date"),
    "finep": (scan_finep, write_finep, "dataset_type"),
}


def summarize(source: str, rows: list[dict], group_key: str) -> None:
    groups: dict[str, list[int]] = {}
    for r in rows:
        g = groups.setdefault(str(r[group_key]), [0, 0])
        g[0] += 1
        g[1] += r["file_size_bytes"] or 0
    log.info("[%s] %d files found", source, len(rows))
    for key in sorted(groups):
        count, size = groups[key]
        log.info("[%s]   %s: %d files, %.2f GB", source, key, count, size / 1024**3)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", action="append", choices=sorted(SOURCES), help="repeatable; default: all")
    parser.add_argument("--replace", action="store_true",
                        help="delete every existing row of each selected manifest before inserting")
    parser.add_argument("--dry-run", action="store_true", help="scan and report only")
    args = parser.parse_args()

    scanned = {}
    for source in args.source or sorted(SOURCES):
        scan, _, group_key = SOURCES[source]
        scanned[source] = scan()
        summarize(source, scanned[source], group_key)
        if not scanned[source]:
            log.warning("[%s] nothing found; check the bronze location before writing", source)

    if args.dry_run:
        log.info("Dry run: no database changes.")
        return 0

    with db_connect() as conn, conn.cursor() as cur:
        for source, rows in scanned.items():
            if not rows:
                log.warning("[%s] skipped (no files), existing rows left untouched", source)
                continue
            SOURCES[source][1](cur, rows, args.replace)
            log.info("[%s] wrote %d manifest rows%s", source, len(rows), " (replaced)" if args.replace else "")
    log.info("Committed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
