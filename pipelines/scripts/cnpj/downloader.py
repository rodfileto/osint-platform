"""
Receita Federal CNPJ releases → MinIO.

The registry is published as a public Nextcloud share, read over WebDAV:
https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9
"""

import logging
import os
import re
import sys
import xml.etree.ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

WEBDAV_BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/webdav"
SHARE_TOKEN = "YggdBLfdninEJX9"
DAV = {"d": "DAV:"}

logger = logging.getLogger(__name__)


def _session() -> requests.Session:
    session = requests.Session()
    session.auth = HTTPBasicAuth(SHARE_TOKEN, "")
    return session


def s3_client():
    import boto3
    from botocore.client import Config

    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))
    if not access_key or not secret_key:
        raise RuntimeError("MinIO credentials not set (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def raw_bucket() -> str:
    return os.getenv("MINIO_BUCKET_RAW", "osint-raw")


def raw_prefix() -> str:
    return os.getenv("MINIO_PREFIX_CNPJ_RAW", "cnpj/raw").rstrip("/")


def processed_bucket() -> str:
    return os.getenv("MINIO_BUCKET_PROCESSED", "osint-processed")


def silver_prefix() -> str:
    return os.getenv("MINIO_PREFIX_CNPJ_SILVER", "cnpj/silver").rstrip("/")


def _propfind(session: requests.Session, url: str) -> ET.Element:
    response = session.request("PROPFIND", url, headers={"Depth": "1"})
    response.raise_for_status()
    return ET.fromstring(response.content)


def list_available_months() -> list[str]:
    """Release folders on the server, oldest first (YYYY-MM)."""
    root = _propfind(_session(), WEBDAV_BASE_URL + "/")
    names = (h.text.strip("/").split("/")[-1] for h in root.iterfind("d:response/d:href", DAV))
    return sorted(n for n in names if re.fullmatch(r"\d{4}-\d{2}", n))


def list_server_files(month: str) -> dict[str, dict]:
    """ZIPs in a release folder: {name: {size, url}}."""
    root = _propfind(_session(), f"{WEBDAV_BASE_URL}/{month}/")
    files = {}
    for item in root.iterfind("d:response", DAV):
        prop = item.find("d:propstat/d:prop", DAV)
        if prop is None or len(prop.find("d:resourcetype", DAV) or []) > 0:
            continue
        path = item.find("d:href", DAV).text
        name = path.split("/")[-1]
        size = prop.find("d:getcontentlength", DAV)
        files[name] = {"size": int(size.text) if size is not None else 0,
                       "url": f"https://arquivos.receitafederal.gov.br{path}"}
    return files


def ensure_bucket(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def ensure_raw_bucket(s3) -> None:
    ensure_bucket(s3, raw_bucket())


def list_minio_objects(prefix: str) -> list[dict]:
    """Every object under prefix, as returned by list_objects_v2."""
    s3 = s3_client()
    ensure_raw_bucket(s3)
    paginator = s3.get_paginator("list_objects_v2")
    return [obj for page in paginator.paginate(Bucket=raw_bucket(), Prefix=prefix) for obj in page.get("Contents", [])]


def missing_files(month: str) -> tuple[dict[str, dict], list[str]]:
    """Server files for the month, and the names absent from MinIO or with a different size."""
    server = list_server_files(month)
    stored = {o["Key"].rsplit("/", 1)[-1]: o["Size"] for o in list_minio_objects(f"{raw_prefix()}/{month}/")}
    return server, sorted(name for name, info in server.items() if stored.get(name) != info["size"])


class _Progress:
    """File-like wrapper that advances a progress bar as the upload reads it."""

    def __init__(self, stream, bar):
        self._stream, self._bar = stream, bar

    def read(self, size=-1):
        chunk = self._stream.read(size)
        self._bar.update(len(chunk))
        return chunk


def stream_to_minio(month: str, name: str, info: dict) -> None:
    """Stream one ZIP from the server into MinIO without touching local disk."""
    from boto3.s3.transfer import TransferConfig

    key = f"{raw_prefix()}/{month}/{name}"
    s3 = s3_client()
    ensure_raw_bucket(s3)
    with _session().get(info["url"], stream=True) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        bar = tqdm(total=info["size"], unit="B", unit_scale=True, desc=name) if sys.stderr.isatty() else None
        stream = _Progress(response.raw, bar) if bar else response.raw
        try:
            # put_object needs a seekable body to sign it over plain HTTP; upload_fileobj
            # buffers the network stream into multipart chunks instead.
            s3.upload_fileobj(stream, raw_bucket(), key,
                              Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=4))
        finally:
            if bar:
                bar.close()
    uploaded = s3.head_object(Bucket=raw_bucket(), Key=key)["ContentLength"]
    if uploaded != info["size"]:
        raise ValueError(f"{name}: uploaded {uploaded} bytes, expected {info['size']}")
    logger.info(f"Uploaded s3://{raw_bucket()}/{key} ({uploaded:,} bytes)")
