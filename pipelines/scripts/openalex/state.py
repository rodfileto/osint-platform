"""Operational state of the OpenAlex lake in PostgreSQL (schema openalex): what is synced and built.

No OpenAlex data goes to PostgreSQL; these tables only let the DAG and a person see which bronze
files are in MinIO and which silver files were built from which bronze version (source ETag).
"""

import os
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

RELEASE_MARKS = ("synced_at", "verified_at", "silver_built_at", "catalog_built_at")


def connect():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "osint_metadata"),
        user=os.environ.get("POSTGRES_USER", "osint_admin"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )
    conn.autocommit = True
    return conn


def latest_release(conn) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT release_date, {', '.join(RELEASE_MARKS)} FROM openalex.release "
                    "ORDER BY release_date DESC LIMIT 1")
        row = cur.fetchone()
    return dict(zip(("release_date", *RELEASE_MARKS), row)) if row else None


def mark_release(conn, release_date: date | str, mark: str) -> None:
    if mark not in RELEASE_MARKS:
        raise ValueError(mark)
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO openalex.release (release_date, {mark}) VALUES (%s, now()) "
                    f"ON CONFLICT (release_date) DO UPDATE SET {mark} = now()", (release_date,))


def bronze_files(conn, entity: str | None = None) -> dict[str, tuple[int, str]]:
    """(size, source ETag) of every bronze file, keyed by its path relative to data/parquet/."""
    with conn.cursor() as cur:
        cur.execute("SELECT key, size_bytes, source_etag FROM openalex.bronze_file"
                    + (" WHERE entity = %s" if entity else ""), (entity,) if entity else ())
        return {key: (size, etag) for key, size, etag in cur.fetchall()}


def record_bronze(conn, files, release_date) -> None:
    rows = [(f.rel_path, f.entity, f.size, f.records, f.etag, release_date) for f in files]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO openalex.bronze_file (key, entity, size_bytes, record_count, source_etag, release_date)
            VALUES %s
            ON CONFLICT (key) DO UPDATE SET entity = EXCLUDED.entity, size_bytes = EXCLUDED.size_bytes,
                record_count = EXCLUDED.record_count, source_etag = EXCLUDED.source_etag,
                release_date = EXCLUDED.release_date, synced_at = now()
        """, rows)


def forget_bronze(conn, keys: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM openalex.bronze_file WHERE key = ANY(%s)", (keys,))


def silver_files(conn, table: str) -> dict[str, str]:
    """Source ETag each silver file of a table was built from, keyed by bronze file."""
    with conn.cursor() as cur:
        cur.execute("SELECT source_key, source_etag FROM openalex.silver_file WHERE silver_table = %s", (table,))
        return dict(cur.fetchall())


def record_silver(conn, table: str, source_key: str, source_etag: str, rows: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO openalex.silver_file (silver_table, source_key, source_etag, rows) VALUES (%s, %s, %s, %s)
            ON CONFLICT (silver_table, source_key) DO UPDATE SET source_etag = EXCLUDED.source_etag,
                rows = EXCLUDED.rows, built_at = now()
        """, (table, source_key, source_etag, rows))


def forget_silver(conn, table: str, keys: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM openalex.silver_file WHERE silver_table = %s AND source_key = ANY(%s)",
                    (table, keys))
