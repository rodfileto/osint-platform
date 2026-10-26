"""CNPJ transform step: one ZIP at a time from MinIO, through DuckDB SQL, to Parquet.

Each Parquet file is kept locally for the load and copied to MinIO as silver:
s3://osint-processed/cnpj/silver/<entity>/reference_month=<YYYY-MM>/<file>.parquet
"""

import io
import logging
import os
import re
import shutil
import sys
import time
import zipfile
from pathlib import Path

import duckdb

from .config import PROCESSED_PATH, STAGING_PATH, TEMP_SSD_PATH

sys.path.insert(0, "/opt/airflow/scripts/cnpj")

logger = logging.getLogger(__name__)

REFERENCE_TYPES = {
    "Cnaes": "cnaes",
    "Motivos": "motivos",
    "Municipios": "municipios",
    "Naturezas": "naturezas",
    "Paises": "paises",
    "Qualificacoes": "qualificacoes",
}
PARTITIONED = re.compile(r"(Empresas|Estabelecimentos|Socios)(\d+)\.zip")
DUCKDB_MEMORY_LIMIT = os.getenv("CNPJ_DUCKDB_MEMORY_LIMIT", "4GB")


def parquet_name(file_name: str) -> str:
    match = PARTITIONED.fullmatch(file_name)
    if match:
        return f"{match.group(1).lower()}_{match.group(2)}.parquet"
    stem = file_name.removesuffix(".zip")
    if stem == "Simples" or stem in REFERENCE_TYPES:
        return f"{stem.lower()}.parquet"
    raise ValueError(f"Unknown CNPJ file: {file_name}")


def silver_key(reference_month: str, file_name: str) -> str:
    import downloader

    name = parquet_name(file_name)
    entity = re.sub(r"_\d+$", "", name.removesuffix(".parquet"))
    return f"{downloader.silver_prefix()}/{entity}/reference_month={reference_month}/{name}"


def _upload_silver(reference_month: str, file_name: str, parquet: Path) -> None:
    import downloader

    s3 = downloader.s3_client()
    bucket, key = downloader.processed_bucket(), silver_key(reference_month, file_name)
    downloader.ensure_bucket(s3, bucket)
    s3.upload_file(str(parquet), bucket, key)
    logger.info(f"{parquet.name} → s3://{bucket}/{key}")


def _sync_silver(reference_month: str, file_name: str, parquet: Path) -> bool:
    """Make the local Parquet and its silver copy both exist. False when neither does."""
    import downloader
    from botocore.exceptions import ClientError

    s3 = downloader.s3_client()
    bucket, key = downloader.processed_bucket(), silver_key(reference_month, file_name)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        in_silver = True
    except ClientError:
        in_silver = False

    if parquet.exists():
        if not in_silver:
            _upload_silver(reference_month, file_name, parquet)
        return True
    if in_silver:
        parquet.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(parquet))
        logger.info(f"{parquet.name} restored from s3://{bucket}/{key}")
        return True
    return False


def _query(file_name: str, csv_path: str, output_path: str) -> str:
    import cleaners

    match = PARTITIONED.fullmatch(file_name)
    if match:
        builder = getattr(cleaners, f"build_{match.group(1).lower()}_query")
        return builder(csv_path, output_path)
    stem = file_name.removesuffix(".zip")
    if stem == "Simples":
        return cleaners.build_simples_query(csv_path, output_path)
    return cleaners.build_reference_query(csv_path, output_path, REFERENCE_TYPES[stem])


def extract_csv(zip_path: Path, csv_path: Path) -> None:
    """Write the single CSV inside a Receita ZIP to csv_path as UTF-8."""
    with zipfile.ZipFile(zip_path) as archive:
        members = [m for m in archive.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected one CSV inside, found {len(members)}")
        # Receita publishes Latin-1. DuckDB's own latin-1 reader rejects some valid bytes
        # (0x1A, 0x8F) and its default UTF-8 reader drops accented rows, so decode here.
        with archive.open(members[0]) as raw, open(csv_path, "w", encoding="utf-8", newline="") as out:
            shutil.copyfileobj(io.TextIOWrapper(raw, encoding="latin-1", newline=""), out, 16 * 1024 * 1024)


def files_for_month(reference_month: str) -> list[str]:
    from manifest_tracker import get_db_connection

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name FROM cnpj.download_manifest WHERE reference_month = %s ORDER BY file_name",
                (reference_month,),
            )
            files = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    known = [f for f in files if PARTITIONED.fullmatch(f) or f.removesuffix(".zip") in {"Simples", *REFERENCE_TYPES}]
    logger.info(f"{reference_month}: {len(known)} files to transform")
    return known


def _download(reference_month: str, file_name: str, target: Path) -> None:
    import downloader

    key = f"{downloader.raw_prefix()}/{reference_month}/{file_name}"
    downloader.s3_client().download_file(downloader.raw_bucket(), key, str(target))


def transform_file(reference_month: str, file_name: str, force_reprocess: bool) -> dict:
    from manifest_tracker import is_file_processed, mark_extracted, mark_failed, mark_transformed

    parquet = PROCESSED_PATH / reference_month / parquet_name(file_name)
    if (
        not force_reprocess
        and is_file_processed(reference_month, file_name, "transformed")
        and _sync_silver(reference_month, file_name, parquet)
    ):
        logger.info(f"{file_name} already transformed: {parquet}")
        return {"file_name": file_name, "skipped": True}

    work_dir = STAGING_PATH / reference_month / file_name.removesuffix(".zip")
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True)
    try:
        zip_path = work_dir / file_name
        _download(reference_month, file_name, zip_path)
        csv_path = work_dir / (file_name.removesuffix(".zip") + ".csv")
        extract_csv(zip_path, csv_path)
        zip_path.unlink()
        mark_extracted(reference_month, file_name, str(csv_path))

        parquet.parent.mkdir(parents=True, exist_ok=True)
        parquet.unlink(missing_ok=True)
        spill_dir = TEMP_SSD_PATH / "duckdb_tmp"
        spill_dir.mkdir(parents=True, exist_ok=True)
        start = time.time()
        con = duckdb.connect(config={"temp_directory": str(spill_dir), "memory_limit": DUCKDB_MEMORY_LIMIT})
        try:
            con.execute(_query(file_name, str(csv_path), str(parquet)))
            rows = con.execute(f"SELECT count(*) FROM '{parquet}'").fetchone()[0]
        finally:
            con.close()
        duration = time.time() - start
        _upload_silver(reference_month, file_name, parquet)
        mark_transformed(reference_month, file_name, str(parquet), rows, duration)
        logger.info(f"{file_name} → {parquet.name}: {rows:,} rows in {duration:.1f}s")
        return {"file_name": file_name, "rows": rows, "skipped": False}
    except Exception as exc:
        mark_failed(reference_month, file_name, str(exc))
        raise
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
