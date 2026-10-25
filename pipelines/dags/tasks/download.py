"""CNPJ download step: stream a Receita Federal release to MinIO and pick the month to process."""

import logging
import os
import re
import sys
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, "/opt/airflow/scripts/cnpj")

MAIN_FILE_TYPES = ("empresas", "estabelecimentos", "socios")
REFERENCE_FILES = {"Simples", "Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes"}

logger = logging.getLogger(__name__)


def requested_month(value: Optional[str]) -> Optional[str]:
    """Normalize the reference_month param: None for 'auto', else a validated YYYY-MM."""
    value = (value or "").strip()
    if value.lower() in {"", "auto", "none", "null"}:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError(f"Invalid reference_month '{value}'. Expected YYYY-MM (e.g. 2026-10) or 'auto'.")
    return value


def db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def download_release(reference_month: Optional[str]) -> Optional[str]:
    """Make the month (requested, or the latest on the server) complete in MinIO."""
    import downloader

    month = requested_month(reference_month)
    if month is None:
        available = downloader.list_available_months()
        if not available:
            logger.warning("No releases listed on the Receita Federal server")
            return None
        month = available[-1]

    for attempt in (1, 2):
        server, missing = downloader.missing_files(month)
        if not server:
            raise ValueError(f"The Receita Federal server lists no files for {month}")
        if not missing:
            logger.info(f"{month} is complete in MinIO ({len(server)} files)")
            return month
        logger.info(f"{month}: {len(missing)}/{len(server)} files missing in MinIO (attempt {attempt}): {missing}")
        for name in missing:
            try:
                downloader.stream_to_minio(month, name, server[name])
            except Exception as exc:
                logger.error(f"{name}: {exc}")

    _, missing = downloader.missing_files(month)
    if missing:
        raise RuntimeError(f"{month} still incomplete in MinIO, missing: {missing}")
    return month


def file_type(file_name: str) -> str:
    match = re.fullmatch(r"(Empresas|Estabelecimentos|Socios)\d+\.zip", file_name)
    if match:
        return match.group(1).lower()
    return "reference" if file_name.removesuffix(".zip") in REFERENCE_FILES else "unknown"


def update_manifest() -> None:
    """Register every ZIP in MinIO in cnpj.download_manifest.

    Progress columns are kept, unless the object's size changed (a re-published file),
    in which case they are cleared so the file is transformed and loaded again.
    """
    import downloader

    prefix = downloader.raw_prefix() + "/"
    rows = []
    for obj in downloader.list_minio_objects(prefix):
        parts = obj["Key"][len(prefix):].split("/")
        if len(parts) == 2 and re.fullmatch(r"\d{4}-\d{2}", parts[0]) and parts[1].endswith(".zip"):
            month, name = parts
            rows.append((month, name, file_type(name), obj["Size"], obj["LastModified"].replace(tzinfo=None), "downloaded"))

    reset = ", ".join(
        f"{col} = CASE WHEN m.file_size_bytes IS DISTINCT FROM EXCLUDED.file_size_bytes THEN NULL ELSE m.{col} END"
        for col in ("extracted_at", "transformed_at", "loaded_postgres_at", "loaded_neo4j_at")
    )
    if not rows:
        logger.warning(f"No ZIPs under s3://{downloader.raw_bucket()}/{prefix}")
        return
    conn = db_connection()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, f"""
                INSERT INTO cnpj.download_manifest AS m
                    (reference_month, file_name, file_type, file_size_bytes, download_date, processing_status)
                VALUES %s
                ON CONFLICT (reference_month, file_name) DO UPDATE SET
                    {reset},
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    download_date = EXCLUDED.download_date,
                    updated_at = CURRENT_TIMESTAMP
            """, rows)
            cur.execute("SELECT reference_month, count(*) FROM cnpj.download_manifest GROUP BY 1 ORDER BY 1")
            for month, count in cur.fetchall():
                logger.info(f"manifest {month}: {count} files")
    finally:
        conn.close()


def select_month(reference_month: Optional[str], force_reprocess: bool) -> Optional[str]:
    """Return the month to process, or None when it is already fully loaded."""
    month = requested_month(reference_month)
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            if month is None:
                cur.execute("SELECT max(reference_month) FROM cnpj.download_manifest")
                month = cur.fetchone()[0]
            if month is None:
                logger.info("Manifest is empty; nothing to process")
                return None
            cur.execute(
                """
                SELECT count(*) FILTER (WHERE transformed_at IS NULL OR loaded_postgres_at IS NULL),
                       count(*)
                FROM cnpj.download_manifest
                WHERE reference_month = %s AND file_type IN %s
                """,
                (month, MAIN_FILE_TYPES),
            )
            pending, total = cur.fetchone()
    finally:
        conn.close()

    if total == 0:
        raise ValueError(f"No files registered for {month}; download it first")
    if force_reprocess:
        logger.info(f"force_reprocess: processing {month} again")
        return month
    if pending == 0:
        logger.info(f"{month} is already transformed and loaded; nothing to do")
        return None
    logger.info(f"{month}: {pending}/{total} main files still to transform or load")
    return month
