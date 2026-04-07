"""
INPI Patents Postgres Loader

Carrega os 10 CSVs de patentes do INPI (armazenados no MinIO pelo
inpi_download DAG) nas tabelas do schema inpi no PostgreSQL.

Estratégia de carga:
  TRUNCATE inpi.patentes_dados_bibliograficos CASCADE
    → limpa todas as 10 tabelas em cascata (chaves estrangeiras)
  INSERT dados_bibliograficos (tabela pai)
  INSERT das 9 tabelas filhas (em qualquer ordem após o pai)
  REFRESH MATERIALIZED VIEW CONCURRENTLY inpi.mv_patent_search

Transformações aplicadas no load:
  tipo_patente      ← regex sobre numero_inpi (prefixo alfabético: PI, MU, PP…)
  sigilo            ← float > 0 → True, senão False
  cnpj_basico_resolved ← LEFT(cgccpf, 8) quando tipo = pessoa jurídica e valor
                         é CNPJ numérico de 14 dígitos; NULL caso contrário

Codificação CSV:
  Tenta UTF-8 / UTF-8-BOM primeiro; fallback para Latin-1 (ISO-8859-1).
"""

from __future__ import annotations

import io
import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000   # linhas por batch de INSERT


# ---------------------------------------------------------------------------
# Infraestrutura: banco e MinIO
# ---------------------------------------------------------------------------

def _get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )


def _get_minio_client():
    import boto3  # type: ignore[import-untyped]
    from botocore.client import Config  # type: ignore[import-untyped]

    endpoint_url = os.getenv(
        "MINIO_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    )
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials not set. "
            "Expected MINIO_ROOT_USER/MINIO_ROOT_PASSWORD (or MINIO_ACCESS_KEY/MINIO_SECRET_KEY)."
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


def _read_csv_from_minio(minio_info: dict) -> bytes:
    s3 = _get_minio_client()
    bucket = minio_info["minio_bucket"]
    key = minio_info["minio_key"]
    logger.info(f"Lendo s3://{bucket}/{key}")
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def _read_csv_chunks(content: bytes, **kwargs):
    """
    Lê CSV em chunks com detecção automática de encoding.
    Tenta UTF-8-BOM e UTF-8 antes de usar Latin-1 como fallback.
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            content[:4096].decode(encoding)
            return pd.read_csv(io.BytesIO(content), encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(io.BytesIO(content), encoding="latin-1", **kwargs)


# ---------------------------------------------------------------------------
# Helpers de transformação vetorizada
# ---------------------------------------------------------------------------

def _vstr(s: pd.Series) -> list[Optional[str]]:
    """Limpa strings: NaN e '' → None, demais → strip."""
    def _clean(x):
        if pd.isna(x):
            return None
        v = str(x).strip()
        return v if v else None
    return list(s.apply(_clean))


def _vint(s: pd.Series) -> list[Optional[int]]:
    """Converte Series de strings para int; NaN/inválido → None."""
    def _conv(x):
        if pd.isna(x):
            return None
        try:
            return int(float(str(x).strip()))
        except (TypeError, ValueError):
            return None
    return list(s.apply(_conv))


def _vsmallint(s: pd.Series) -> list[Optional[int]]:
    return _vint(s)


def _vdate(s: pd.Series) -> list[Optional[date]]:
    """Converte Series de strings para date; erros e NaN → None."""
    # errors='coerce' turns blanks and unparseable values into NaT automatically
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return [v.date() if pd.notna(v) else None for v in parsed]


def _vsigilo(s: pd.Series) -> list[bool]:
    """Converte campo float-like para bool: valor > 0 → True."""
    def _conv(x):
        try:
            return float(x) > 0
        except (TypeError, ValueError):
            return False
    return list(s.apply(_conv))


def _vtipo_patente(s: pd.Series) -> list[Optional[str]]:
    """Extrai prefixo alfabético do numero_inpi: PI, MU, PP, MI, DI…"""
    def _extract(x):
        if not isinstance(x, str):
            return None
        m = re.match(r"^([A-Za-z]+)", x.strip())
        return m.group(1).upper() if m else None
    return list(s.apply(_extract))


def _vcnpj_basico_pairs(cgccpf_s: pd.Series, tipo_s: pd.Series) -> list[Optional[str]]:
    """Versão iterativa de _vcnpj_basico para uso com duas colunas."""
    result = []
    for cgccpf, tipo in zip(cgccpf_s, tipo_s):
        if not isinstance(tipo, str):
            result.append(None)
            continue
        if "jur" not in tipo.lower():
            result.append(None)
            continue
        if not isinstance(cgccpf, str):
            result.append(None)
            continue
        cgccpf_s_val = cgccpf.strip()
        if re.match(r"^\d{14}$", cgccpf_s_val):
            result.append(cgccpf_s_val[:8])
        else:
            result.append(None)
    return result


# ---------------------------------------------------------------------------
# Manifesto: leitura e atualização
# ---------------------------------------------------------------------------

def get_snapshot_info() -> dict[str, dict]:
    """
    Retorna {dataset_name: {id, minio_key, minio_bucket, snapshot_date}}
    para o snapshot mais recente de cada dataset com status='downloaded'.
    """
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (dataset_name)
                    id, dataset_name, minio_key, minio_bucket, snapshot_date
                FROM inpi.download_manifest
                WHERE processing_status = 'downloaded'
                ORDER BY dataset_name, snapshot_date DESC, download_date DESC
            """)
            rows = cur.fetchall()
        return {row["dataset_name"]: dict(row) for row in rows}
    finally:
        conn.close()


def check_snapshot_available() -> bool:
    """
    Retorna True se há pelo menos um dataset com status='downloaded'.
    Usado como condição no ShortCircuitOperator.
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM inpi.download_manifest "
                "WHERE processing_status = 'downloaded'"
            )
            row = cur.fetchone()
            count = row[0] if row else 0
        if count == 0:
            logger.info("Nenhum download pendente — ShortCircuit irá encerrar a DAG.")
            return False
        logger.info(f"{count} registro(s) com status='downloaded' — prosseguindo com carga.")
        return True
    finally:
        conn.close()


def mark_datasets_loaded(manifest_ids: list[int], rows_loaded: int) -> None:
    """Atualiza status de 'downloaded' para 'loaded' nos manifests indicados."""
    if not manifest_ids:
        return
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inpi.download_manifest
                SET processing_status = 'loaded',
                    row_count         = %s,
                    updated_at        = CURRENT_TIMESTAMP
                WHERE id = ANY(%s)
                """,
                (rows_loaded, manifest_ids),
            )
        conn.commit()
        logger.info(f"Manifests {manifest_ids} atualizados para 'loaded'.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_all_loaded(snapshot_info: dict[str, dict], rows_per_dataset: dict[str, int]) -> None:
    """
    Marca todos os datasets do snapshot como 'loaded' na tabela de manifesto.
    rows_per_dataset: {dataset_name: linhas_carregadas}
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            for dataset_name, info in snapshot_info.items():
                rows = rows_per_dataset.get(dataset_name, 0)
                cur.execute(
                    """
                    UPDATE inpi.download_manifest
                    SET processing_status = 'loaded',
                        row_count         = %s,
                        updated_at        = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (rows, info["id"]),
                )
        conn.commit()
        logger.info("Todos os manifests INPI atualizados para 'loaded'.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# TRUNCATE
# ---------------------------------------------------------------------------

def truncate_all_tables() -> None:
    """
    Limpa todas as 10 tabelas de patentes via CASCADE na tabela pai.
    Seguro chamar antes de qualquer carga: os filhos são apagados em cascata.
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE inpi.patentes_dados_bibliograficos CASCADE"
            )
        conn.commit()
        logger.info("TRUNCATE CASCADE — todas as tabelas INPI limpas.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-table loaders
# ---------------------------------------------------------------------------

def _bulk_insert(conn, cur, insert_sql: str, rows: list) -> None:
    """Executa execute_values e commita."""
    if not rows:
        return
    psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=1000)
    conn.commit()


def _fetch_valid_ids(conn) -> frozenset:
    """
    Retorna frozenset com todos os codigo_interno presentes em
    patentes_dados_bibliograficos. Usado para filtrar FK inválidas em
    tabelas filhas — a fonte INPI pode conter referências que não estão
    na tabela bibliográfica (dados históricos/depurados).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT codigo_interno FROM inpi.patentes_dados_bibliograficos")
        return frozenset(row[0] for row in cur.fetchall())


def load_dados_bibliograficos(snapshot_info: dict, snapshot_date: date) -> int:
    """
    Carrega patentes_dados_bibliograficos.
    Deve ser a PRIMEIRA tabela carregada (pai das demais).
    """
    dataset = "patentes_dados_bibliograficos"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_dados_bibliograficos (
            codigo_interno, numero_inpi, tipo_patente,
            data_deposito, data_protocolo, data_publicacao,
            numero_pct, numero_wo, data_publicacao_wo,
            data_entrada_fase_nacional, sigilo, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno) DO NOTHING
    """

    total = 0
    conn = _get_db_connection()
    try:
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            numero_inpi = pd.Series(_vstr(chunk["numero_inpi"]))

            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                numero_inpi,
                _vtipo_patente(numero_inpi),
                _vdate(chunk["data_deposito"]),
                _vdate(chunk["data_protocolo"]),
                _vdate(chunk["data_publicacao"]),
                _vstr(chunk["numero_pct"]),
                _vstr(chunk["numero_wo"]),
                _vdate(chunk["data_publicacao_wo"]),
                _vdate(chunk["data_entrada_fase_nacional"]),
                _vsigilo(chunk["sigilo"]),
                [snapshot_date] * len(chunk),
            ))

            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
            logger.info(f"[{dataset}] {total:,} linhas inseridas...")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas")
    return total


def load_conteudo(snapshot_info: dict, snapshot_date: date) -> int:
    """Carrega patentes_conteudo (título e resumo)."""
    dataset = "patentes_conteudo"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_conteudo (
            codigo_interno, numero_inpi, titulo, resumo, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vstr(chunk["numero_inpi"]),
                _vstr(chunk["titulo"]),
                _vstr(chunk["resumo"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_inventores(snapshot_info: dict, snapshot_date: date) -> int:
    """Carrega patentes_inventores."""
    dataset = "patentes_inventores"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_inventores (
            codigo_interno, ordem, autor, pais, estado, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno, ordem) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vsmallint(chunk["ordem"]),
                _vstr(chunk["autor"]),
                _vstr(chunk["pais"]),
                _vstr(chunk["estado"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_depositantes(snapshot_info: dict, snapshot_date: date) -> int:
    """
    Carrega patentes_depositantes.
    Nota: CSV tem data_inicio_depositante / data_fim_depositante;
    a tabela usa data_inicio / data_fim.
    Resolve cnpj_basico_resolved a partir de cgccpfdepositante + tipopessoadepositante.
    """
    dataset = "patentes_depositantes"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_depositantes (
            codigo_interno, ordem, pais, estado,
            depositante, cgccpfdepositante, tipopessoadepositante,
            data_inicio, data_fim, cnpj_basico_resolved, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno, ordem) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            cgccpf_list = _vstr(chunk["cgccpfdepositante"])
            tipo_list = _vstr(chunk["tipopessoadepositante"])
            cnpj_resolved = _vcnpj_basico_pairs(
                pd.Series(cgccpf_list), pd.Series(tipo_list)
            )
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vsmallint(chunk["ordem"]),
                _vstr(chunk["pais"]),
                _vstr(chunk["estado"]),
                _vstr(chunk["depositante"]),
                cgccpf_list,
                tipo_list,
                _vdate(chunk["data_inicio_depositante"]),
                _vdate(chunk["data_fim_depositante"]),
                cnpj_resolved,
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_classificacao_ipc(snapshot_info: dict, snapshot_date: date) -> int:
    """Carrega patentes_classificacao_ipc."""
    dataset = "patentes_classificacao_ipc"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_classificacao_ipc (
            codigo_interno, ordem, simbolo, versao, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno, ordem) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vsmallint(chunk["ordem"]),
                _vstr(chunk["simbolo"]),
                _vstr(chunk["versao"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_despachos(snapshot_info: dict, snapshot_date: date) -> int:
    """
    Carrega patentes_despachos.
    PK é BIGSERIAL (auto-gerado) — não incluído no INSERT.
    """
    dataset = "patentes_despachos"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_despachos (
            codigo_interno, numero_rpi, data_rpi,
            codigo_despacho, complemento_despacho, snapshot_date
        ) VALUES %s
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vint(chunk["numero_rpi"]),
                _vdate(chunk["data_rpi"]),
                _vstr(chunk["codigo_despacho"]),
                _vstr(chunk["complemento_despacho"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
            logger.info(f"[{dataset}] {total:,} linhas inseridas...")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_procuradores(snapshot_info: dict, snapshot_date: date) -> int:
    """
    Carrega patentes_procuradores.
    PK é BIGSERIAL — não incluído no INSERT.
    CSV tem data_inicio_procurador; tabela usa data_inicio.
    Resolve cnpj_basico_resolved.
    """
    dataset = "patentes_procuradores"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_procuradores (
            codigo_interno, procurador, cgccpfprocurador,
            tipopessoaprocurador, data_inicio, cnpj_basico_resolved, snapshot_date
        ) VALUES %s
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            cgccpf_list = _vstr(chunk["cgccpfprocurador"])
            tipo_list = _vstr(chunk["tipopessoaprocurador"])
            cnpj_resolved = _vcnpj_basico_pairs(
                pd.Series(cgccpf_list), pd.Series(tipo_list)
            )
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vstr(chunk["procurador"]),
                cgccpf_list,
                tipo_list,
                _vdate(chunk["data_inicio_procurador"]),
                cnpj_resolved,
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_prioridades(snapshot_info: dict, snapshot_date: date) -> int:
    """Carrega patentes_prioridades (prioridades unionistas)."""
    dataset = "patentes_prioridades"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_prioridades (
            codigo_interno, pais_prioridade, numero_prioridade,
            data_prioridade, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno, pais_prioridade, numero_prioridade) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vstr(chunk["pais_prioridade"]),
                _vstr(chunk["numero_prioridade"]),
                _vdate(chunk["data_prioridade"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_vinculos(snapshot_info: dict, snapshot_date: date) -> int:
    """
    Carrega patentes_vinculos.
    Ambas as colunas (derivado E origem) referenciam patentes_dados_bibliograficos.
    Apenas vínculos onde ambos os códigos existem na tabela pai são inseridos.
    """
    dataset = "patentes_vinculos"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_vinculos (
            codigo_interno_derivado, codigo_interno_origem,
            data_vinculo, tipo_vinculo, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno_derivado, codigo_interno_origem, tipo_vinculo) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno_derivado"]),
                _vint(chunk["codigo_interno_origem"]),
                _vdate(chunk["data_vinculo"]),
                _vstr(chunk["tipo_vinculo"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            # vinculos: both FK columns must be valid
            rows = [r for r in rows if r[0] in valid_ids and r[1] in valid_ids]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


def load_renumeracoes(snapshot_info: dict, snapshot_date: date) -> int:
    """Carrega patentes_renumeracoes (mapeamento de números antigos)."""
    dataset = "patentes_renumeracoes"
    content = _read_csv_from_minio(snapshot_info[dataset])

    insert_sql = """
        INSERT INTO inpi.patentes_renumeracoes (
            codigo_interno, numero_inpi_original, snapshot_date
        ) VALUES %s
        ON CONFLICT (codigo_interno, numero_inpi_original) DO NOTHING
    """

    total = 0
    skipped = 0
    conn = _get_db_connection()
    try:
        valid_ids = _fetch_valid_ids(conn)
        logger.info(f"[{dataset}] {len(valid_ids):,} codigo_interno válidos")
        for chunk in _read_csv_chunks(
            content, dtype=str, chunksize=CHUNK_SIZE, low_memory=False
        ):
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            rows = list(zip(
                _vint(chunk["codigo_interno"]),
                _vstr(chunk["numero_inpi_original"]),
                [snapshot_date] * len(chunk),
            ))
            before = len(rows)
            rows = [r for r in rows if r[0] in valid_ids and r[1] is not None]
            skipped += before - len(rows)
            with conn.cursor() as cur:
                _bulk_insert(conn, cur, insert_sql, rows)
            total += len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"[{dataset}] Carga concluída: {total:,} linhas ({skipped:,} ignoradas por FK)")
    return total


# ---------------------------------------------------------------------------
# Materialized view
# ---------------------------------------------------------------------------

def refresh_matview() -> None:
    """
    Atualiza a materialized view mv_patent_search.
    Usa REFRESH CONCURRENTLY nas cargas subsequentes (requer unique index — já criado).
    Na primeira carga (view não populada), usa REFRESH simples.
    """
    # Query de inspeção em conexão separada (transação normal)
    conn_check = _get_db_connection()
    try:
        with conn_check.cursor() as cur:
            cur.execute(
                "SELECT ispopulated FROM pg_matviews "
                "WHERE schemaname = 'inpi' AND matviewname = 'mv_patent_search'"
            )
            row = cur.fetchone()
            is_populated = row[0] if row else False
    finally:
        conn_check.close()

    # REFRESH em conexão com autocommit=True definido ANTES de qualquer query
    conn_refresh = _get_db_connection()
    conn_refresh.autocommit = True
    try:
        with conn_refresh.cursor() as cur:
            if is_populated:
                logger.info("Atualizando mv_patent_search (CONCURRENTLY)...")
                cur.execute(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY inpi.mv_patent_search"
                )
            else:
                logger.info("Populando mv_patent_search pela primeira vez...")
                cur.execute(
                    "REFRESH MATERIALIZED VIEW inpi.mv_patent_search"
                )
        logger.info("mv_patent_search atualizada com sucesso.")
    finally:
        conn_refresh.close()
