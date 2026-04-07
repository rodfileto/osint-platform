
"""
INPI Patents Downloader

Baixa os arquivos CSV de patentes publicados pelo INPI no portal de dados abertos:
  https://dadosabertos.inpi.gov.br/index/patentes/

Os arquivos são CSVs estáticos atualizados in-place. A mudança é detectada via
HEAD request (ETag / Last-Modified) antes de qualquer download.

Cada execução com mudança detectada gera um snapshot imutável no MinIO:
  osint-raw/inpi/patentes/<YYYYMMDD>/<FILENAME>.csv

onde <YYYYMMDD> é derivado do header Last-Modified do servidor.
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import psycopg2
import requests
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Datasets rastreados
# ---------------------------------------------------------------------------
BASE_URL = "https://dadosabertos.inpi.gov.br/download/patentes"

INPI_DATASETS: dict[str, str] = {
    "patentes_dados_bibliograficos": f"{BASE_URL}/PATENTES_DADOS_BIBLIOGRAFICOS.csv",
    "patentes_conteudo":             f"{BASE_URL}/PATENTES_CONTEUDO.csv",
    "patentes_inventores":           f"{BASE_URL}/PATENTES_INVENTORES.csv",
    "patentes_depositantes":         f"{BASE_URL}/PATENTES_DEPOSITANTES.csv",
    "patentes_classificacao_ipc":    f"{BASE_URL}/PATENTES_CLASSIFICACAO_IPC.csv",
    "patentes_despachos":            f"{BASE_URL}/PATENTES_DESPACHOS.csv",
    "patentes_procuradores":         f"{BASE_URL}/PATENTES_PROCURADORES.csv",
    "patentes_prioridades":          f"{BASE_URL}/PATENTES_PRIORIDADES.csv",
    "patentes_vinculos":             f"{BASE_URL}/PATENTES_VINCULOS.csv",
    "patentes_renumeracoes":         f"{BASE_URL}/PATENTES_RENUMERACOES.csv",
}

# Timeout conservador para arquivos grandes (ex: PATENTES_DESPACHOS ~28 MB)
REQUEST_TIMEOUT = (30, 600)   # (connect, read) em segundos


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------

def _get_minio_s3_client():
    import boto3
    from botocore.client import Config

    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key   = os.getenv("MINIO_ROOT_USER",     os.getenv("MINIO_ACCESS_KEY"))
    secret_key   = os.getenv("MINIO_ROOT_PASSWORD",  os.getenv("MINIO_SECRET_KEY"))

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials not set. Expected MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
            "(or MINIO_ACCESS_KEY/MINIO_SECRET_KEY)."
        )

    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def _ensure_bucket_exists(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' criado.")


def _get_minio_bucket_and_prefix() -> tuple[str, str]:
    bucket = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
    prefix = os.getenv("MINIO_PREFIX_INPI", "inpi/patentes").rstrip("/")
    return bucket, prefix


def _build_minio_key(snapshot_date: date, filename: str) -> str:
    _, prefix = _get_minio_bucket_and_prefix()
    return f"{prefix}/{snapshot_date.strftime('%Y%m%d')}/{filename}"


# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------

def _get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )


# ---------------------------------------------------------------------------
# Utilitários HTTP
# ---------------------------------------------------------------------------

def _parse_last_modified(header_value: Optional[str]) -> Optional[datetime]:
    if not header_value:
        return None
    try:
        return parsedate_to_datetime(header_value)
    except Exception:
        return None


def _sha256_stream(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Detecção de mudança via HEAD
# ---------------------------------------------------------------------------

def check_remote_headers(dataset_name: str, url: str) -> dict:
    """
    Faz uma requisição HEAD para obter metadados sem baixar o arquivo.

    Returns:
        dict com etag, last_modified, content_length
    """
    logger.info(f"[{dataset_name}] HEAD {url}")
    resp = requests.head(url, timeout=REQUEST_TIMEOUT[0], allow_redirects=True)
    resp.raise_for_status()

    return {
        "etag":           resp.headers.get("ETag"),
        "last_modified":  _parse_last_modified(resp.headers.get("Last-Modified")),
        "content_length": int(resp.headers["Content-Length"])
                          if "Content-Length" in resp.headers else None,
    }


def get_last_download_info(dataset_name: str) -> Optional[dict]:
    """
    Retorna as informações do último download bem-sucedido para o dataset.

    Returns:
        dict com http_etag, http_last_modified, snapshot_date ou None
    """
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT http_etag, http_last_modified, snapshot_date,
                       file_checksum_sha256, file_size_bytes
                FROM inpi.download_manifest
                WHERE dataset_name = %s
                  AND processing_status = 'downloaded'
                ORDER BY snapshot_date DESC, download_date DESC
                LIMIT 1
                """,
                (dataset_name,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def has_remote_changed(dataset_name: str, url: str) -> tuple[bool, dict]:
    """
    Verifica se o arquivo remoto mudou em relação ao último download.

    Returns:
        (changed: bool, remote_headers: dict)
    """
    remote = check_remote_headers(dataset_name, url)
    last = get_last_download_info(dataset_name)

    if last is None:
        logger.info(f"[{dataset_name}] Sem histórico — download necessário.")
        return True, remote

    # Comparar ETag (mais confiável quando disponível)
    if remote.get("etag") and last.get("http_etag"):
        changed = remote["etag"] != last["http_etag"]
        logger.info(
            f"[{dataset_name}] ETag: remoto={remote['etag']} "
            f"vs último={last['http_etag']} → {'MUDOU' if changed else 'igual'}"
        )
        return changed, remote

    # Fallback: Last-Modified
    if remote.get("last_modified") and last.get("http_last_modified"):
        changed = remote["last_modified"] > last["http_last_modified"]
        logger.info(
            f"[{dataset_name}] Last-Modified: remoto={remote['last_modified']} "
            f"vs último={last['http_last_modified']} → {'MUDOU' if changed else 'igual'}"
        )
        return changed, remote

    # Sem informação suficiente → baixar por precaução
    logger.warning(f"[{dataset_name}] Sem ETag nem Last-Modified — forçando download.")
    return True, remote


# ---------------------------------------------------------------------------
# Download + upload para MinIO (streaming, sem retenção local)
# ---------------------------------------------------------------------------

def download_and_upload_to_minio(
    dataset_name: str,
    url: str,
    snapshot_date: date,
    force: bool = False,
) -> dict:
    """
    Orquestra o download de um dataset INPI e o upload para o MinIO.

    O arquivo é baixado em memória (todos os datasets somam ~88 MB)
    e enviado ao MinIO sem retenção no disco local do worker.

    Args:
        dataset_name:  Nome lógico do dataset (chave de INPI_DATASETS)
        url:           URL de download
        snapshot_date: Data do snapshot (derivada do Last-Modified)
        force:         Se True, baixa mesmo que o conteúdo não tenha mudado

    Returns:
        dict com resultado da operação
    """
    logger.info(f"[{dataset_name}] Iniciando download de {url}")

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT,
                      allow_redirects=True) as resp:
        resp.raise_for_status()
        content = resp.content

    file_size = len(content)
    checksum = _sha256_stream(content)
    logger.info(f"[{dataset_name}] Baixado: {file_size:,} bytes | SHA-256: {checksum[:16]}...")

    # Inspecionar cabeçalho CSV
    try:
        first_line = content.decode("utf-8", errors="replace").split("\n")[0]
        column_names = [c.strip() for c in first_line.split(",") if c.strip()]
        row_count = max(0, content.count(b"\n") - 1)
    except Exception as exc:
        logger.warning(f"[{dataset_name}] Não foi possível inspecionar CSV: {exc}")
        column_names = None
        row_count = None

    # Upload para MinIO
    file_name = url.rstrip("/").split("/")[-1]
    minio_key = _build_minio_key(snapshot_date, file_name)
    bucket, _ = _get_minio_bucket_and_prefix()

    s3 = _get_minio_s3_client()
    _ensure_bucket_exists(s3, bucket)

    s3.put_object(
        Bucket=bucket,
        Key=minio_key,
        Body=content,
        ContentType="text/csv",
        Metadata={
            "dataset-name":  dataset_name,
            "snapshot-date": snapshot_date.isoformat(),
            "sha256":        checksum,
        },
    )
    logger.info(f"[{dataset_name}] Upload concluído → s3://{bucket}/{minio_key}")

    return {
        "dataset_name":  dataset_name,
        "file_name":     file_name,
        "minio_key":     minio_key,
        "minio_bucket":  bucket,
        "file_size":     file_size,
        "checksum":      checksum,
        "row_count":     row_count,
        "column_names":  column_names,
        "snapshot_date": snapshot_date,
    }


# ---------------------------------------------------------------------------
# Registro no manifesto
# ---------------------------------------------------------------------------

def register_manifest(
    *,
    dataset_name: str,
    source_url: str,
    result: dict,
    remote_headers: dict,
    download_date: datetime,
) -> int:
    """
    Insere um registro bem-sucedido na tabela inpi.download_manifest.

    Returns:
        ID da linha inserida
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inpi.download_manifest (
                    file_name, source_url, dataset_name,
                    minio_key, minio_bucket, snapshot_date,
                    download_date,
                    file_size_bytes, file_checksum_sha256,
                    http_last_modified, http_etag, http_content_length,
                    is_valid, row_count, column_names,
                    processing_status,
                    updated_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s,
                    %s, %s, %s,
                    TRUE, %s, %s,
                    'downloaded',
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (minio_key) DO UPDATE SET
                    file_size_bytes      = EXCLUDED.file_size_bytes,
                    file_checksum_sha256 = EXCLUDED.file_checksum_sha256,
                    http_last_modified   = EXCLUDED.http_last_modified,
                    http_etag            = EXCLUDED.http_etag,
                    is_valid             = EXCLUDED.is_valid,
                    row_count            = EXCLUDED.row_count,
                    column_names         = EXCLUDED.column_names,
                    processing_status    = EXCLUDED.processing_status,
                    updated_at           = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (
                    result["file_name"],
                    source_url,
                    dataset_name,
                    result["minio_key"],
                    result["minio_bucket"],
                    result["snapshot_date"],
                    download_date,
                    result["file_size"],
                    result["checksum"],
                    remote_headers.get("last_modified"),
                    remote_headers.get("etag"),
                    remote_headers.get("content_length"),
                    result["row_count"],
                    result["column_names"],
                ),
            )
            row_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"[{dataset_name}] Manifesto registrado — id={row_id}")
            return row_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def register_failed(
    *,
    dataset_name: str,
    source_url: str,
    error_message: str,
) -> None:
    """Registra uma tentativa de download que falhou."""
    file_name = source_url.rstrip("/").split("/")[-1]
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inpi.download_manifest (
                    file_name, source_url, dataset_name,
                    minio_key, minio_bucket, snapshot_date,
                    download_date, processing_status, error_message,
                    updated_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, CURRENT_DATE,
                    CURRENT_TIMESTAMP, 'failed', %s,
                    CURRENT_TIMESTAMP
                )
                """,
                (
                    file_name,
                    source_url,
                    dataset_name,
                    f"inpi/patentes/failed/{file_name}",
                    "osint-raw",
                    error_message,
                ),
            )
            conn.commit()
            logger.warning(f"[{dataset_name}] Falha registrada no manifesto.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Função principal de coleta — um dataset
# ---------------------------------------------------------------------------

def collect_dataset(
    dataset_name: str,
    url: str,
    force: bool = False,
) -> dict:
    """
    Orquestra a coleta completa de um dataset INPI:
    1. HEAD request para detectar mudança
    2. Download + upload para MinIO se necessário
    3. Registro no manifesto

    Returns:
        dict com resultado da coleta
    """
    logger.info(f"=== Coletando dataset: {dataset_name} ===")

    # 1. Verificar se mudou
    try:
        changed, remote_headers = has_remote_changed(dataset_name, url)
    except Exception as exc:
        logger.warning(f"[{dataset_name}] HEAD falhou ({exc}) — forçando download.")
        changed = True
        remote_headers = {}

    if not changed and not force:
        logger.info(f"[{dataset_name}] Arquivo inalterado — download ignorado.")
        return {"dataset_name": dataset_name, "skipped": True, "reason": "unchanged"}

    # Derivar data do snapshot a partir do Last-Modified (fallback: hoje)
    last_modified_dt: Optional[datetime] = remote_headers.get("last_modified")
    snapshot_date = last_modified_dt.date() if last_modified_dt else datetime.now(tz=timezone.utc).date()

    download_date = datetime.now(tz=timezone.utc)

    # 2. Download → MinIO
    try:
        result = download_and_upload_to_minio(
            dataset_name=dataset_name,
            url=url,
            snapshot_date=snapshot_date,
            force=force,
        )
    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"[{dataset_name}] Download/upload falhou: {error_msg}")
        try:
            register_failed(dataset_name=dataset_name, source_url=url, error_message=error_msg)
        except Exception as db_exc:
            logger.error(f"[{dataset_name}] Falha também ao registrar erro no manifesto: {db_exc}")
        raise

    # 3. Registrar manifesto
    try:
        manifest_id = register_manifest(
            dataset_name=dataset_name,
            source_url=url,
            result=result,
            remote_headers=remote_headers,
            download_date=download_date,
        )
    except Exception as exc:
        # Não propaga — o arquivo já está no MinIO com sucesso
        logger.error(f"[{dataset_name}] Falha ao registrar manifesto: {exc}")
        manifest_id = None

    return {
        "dataset_name":  dataset_name,
        "skipped":       False,
        "minio_key":     result["minio_key"],
        "file_size":     result["file_size"],
        "checksum":      result["checksum"],
        "row_count":     result["row_count"],
        "snapshot_date": snapshot_date.isoformat(),
        "manifest_id":   manifest_id,
    }
