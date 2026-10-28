"""MinIO and DuckDB access for the OpenAlex lake, with the MINIO_* settings the CNPJ pipeline uses."""

import os

import duckdb

SHORT_ID_REGEX = r"^https?://openalex\.org/([a-z-]+s/)?"


def short_id_sql(expr: str) -> str:
    return f"nullif(regexp_replace({expr}, '{SHORT_ID_REGEX}', ''), '')"


def _minio_endpoint() -> tuple[str, bool]:
    url = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")
    return url.replace("https://", "").replace("http://", ""), url.startswith("https://")


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", os.environ.get("MINIO_ACCESS_KEY")),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", os.environ.get("MINIO_SECRET_KEY")),
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
        config=Config(max_pool_connections=32),
    )


def ensure_bucket(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def list_objects(s3, bucket: str, prefix: str) -> dict[str, int]:
    """Size of every object under prefix, keyed by its full key."""
    sizes = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        sizes.update({obj["Key"]: obj["Size"] for obj in page.get("Contents", [])})
    return sizes


def delete_objects(s3, bucket: str, keys: list[str]) -> None:
    for start in range(0, len(keys), 1000):
        batch = [{"Key": key} for key in keys[start:start + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})


def duckdb_connect(lake=None, threads: int | None = None, path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that reads MinIO through httpfs, with the short_id macro."""
    config = {}
    if lake is not None:
        lake.duckdb_temp.mkdir(parents=True, exist_ok=True)
        config = {"memory_limit": lake.duckdb_memory_limit, "temp_directory": str(lake.duckdb_temp)}
    con = duckdb.connect(path, config=config)
    if threads:
        con.execute(f"SET threads = {threads}")
    endpoint, use_ssl = _minio_endpoint()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE OR REPLACE SECRET osint_minio (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
        "URL_STYLE 'path', USE_SSL ?)",
        [
            os.environ.get("MINIO_ROOT_USER", os.environ.get("MINIO_ACCESS_KEY", "")),
            os.environ.get("MINIO_ROOT_PASSWORD", os.environ.get("MINIO_SECRET_KEY", "")),
            endpoint,
            use_ssl,
        ],
    )
    con.execute(f"CREATE OR REPLACE MACRO short_id(url) AS {short_id_sql('url')}")
    return con
