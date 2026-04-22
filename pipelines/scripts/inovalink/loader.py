"""Inovalink Postgres loader.

Collects the public Livewire payload exposed by https://www.inovalink.org/
and persists the normalized result into the inovalink schema.
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests

logger = logging.getLogger(__name__)

ENTRYPOINT_URL = os.getenv("INOVALINK_ENTRYPOINT_URL", "https://www.inovalink.org/")
LIVEWIRE_UPDATE_URL = os.getenv(
    "INOVALINK_LIVEWIRE_UPDATE_URL", "https://www.inovalink.org/livewire/update"
)
REQUEST_TIMEOUT = (30, 120)
ARTIFACT_DIR = Path(
    os.getenv("INOVALINK_ARTIFACT_DIR", "/opt/airflow/data/inovalink/artifacts")
)
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "osint-raw")
MINIO_PREFIX_INOVALINK = os.getenv("MINIO_PREFIX_INOVALINK", "inovalink/livewire").rstrip("/")
GOOGLE_API_KEY_PATTERN = re.compile(r"AIza[0-9A-Za-z\-_]{35}")
REDACTED_GOOGLE_API_KEY = "[REDACTED_GOOGLE_API_KEY]"

COLLECTION_TO_ENTITY_KIND = {
    "incubators": "incubator",
    "accelerators": "accelerator",
    "operationParks": "park",
    "implantationParks": "park",
    "planningParks": "park",
    "incubatedCompanies": "company",
    "acceleratedCompanies": "company",
    "graduatedCompanies": "company",
    "residentCompanies": "company",
}

COLLECTION_TO_CATEGORY_LABEL = {
    "incubators": "Incubadora",
    "accelerators": "Aceleradora",
    "operationParks": "Parque em Operacao",
    "implantationParks": "Parque em Implantacao",
    "planningParks": "Parque em Planejamento",
    "incubatedCompanies": "Empresa Incubada",
    "acceleratedCompanies": "Empresa Acelerada",
    "graduatedCompanies": "Empresa Graduada",
    "residentCompanies": "Empresa Residente",
}

CHILD_COLLECTION_TO_RELATIONSHIP = {
    "incubatedCompanies": "incubated_by",
    "acceleratedCompanies": "accelerated_by",
    "graduatedCompanies": "graduated_from",
    "residentCompanies": "resident_in",
}


def _get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )


def _get_minio_s3_client():
    import boto3  # type: ignore[import-untyped]
    from botocore.client import Config  # type: ignore[import-untyped]

    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY"))

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
    from botocore.exceptions import ClientError  # type: ignore[import-untyped]

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=bucket)
        logger.info("Bucket '%s' criado.", bucket)


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _redact_google_api_keys(value: Any) -> Any:
    if isinstance(value, str):
        return GOOGLE_API_KEY_PATTERN.sub(REDACTED_GOOGLE_API_KEY, value)
    if isinstance(value, list):
        return [_redact_google_api_keys(item) for item in value]
    if isinstance(value, dict):
        return {key: _redact_google_api_keys(item) for key, item in value.items()}
    return value


def _normalize_digits(value: Any, width: int | None = None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    if width is not None:
        if len(digits) > width:
            return None
        digits = digits.zfill(width)
    return digits


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "sim"}:
        return True
    if text in {"0", "false", "f", "no", "n", "nao", "não"}:
        return False
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_decimal(value: Any) -> Decimal | None:
    text = _normalize_text(value)
    if text is None:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if text is None:
        return None
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
        return datetime.strptime(text, "%d/%m/%Y").replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_date(value: Any):
    dt = _parse_timestamp(value)
    return dt.date() if dt else None


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _ensure_artifact_dir(collected_at: datetime) -> Path:
    day_dir = ARTIFACT_DIR / collected_at.strftime("%Y%m%d")
    try:
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir
    except PermissionError:
        fallback_root = Path(tempfile.gettempdir()) / "inovalink-artifacts"
        fallback_day_dir = fallback_root / collected_at.strftime("%Y%m%d")
        fallback_day_dir.mkdir(parents=True, exist_ok=True)
        logger.warning(
            "Sem permissao para gravar em %s; usando diretorio temporario %s.",
            day_dir,
            fallback_day_dir,
        )
        return fallback_day_dir


def _upload_artifact_to_minio(collected_at: datetime, name: str, body: bytes, metadata: dict[str, str]) -> str:
    s3 = _get_minio_s3_client()
    _ensure_bucket_exists(s3, MINIO_BUCKET_RAW)
    key = f"{MINIO_PREFIX_INOVALINK}/{collected_at.strftime('%Y%m%d')}/{name}"
    s3.put_object(
        Bucket=MINIO_BUCKET_RAW,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata=metadata,
    )
    return f"s3://{MINIO_BUCKET_RAW}/{key}"


def _extract_livewire_actor_payload(response_json: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    components = response_json.get("components") or []
    if not components:
        raise RuntimeError("Livewire response returned no components.")
    effects = components[0].get("effects") or {}
    xjs = effects.get("xjs") or []
    if not xjs:
        raise RuntimeError("Livewire response did not return xjs payload.")
    params = xjs[0].get("params") or []
    if not params or not isinstance(params[0], dict):
        raise RuntimeError("Livewire xjs payload did not include actor params.")
    return params[0]


def collect_livewire_payload(*, redact_google_api_keys: bool = False) -> dict[str, Any]:
    session = requests.Session()
    collected_at = datetime.now(timezone.utc)

    homepage = session.get(ENTRYPOINT_URL, timeout=REQUEST_TIMEOUT)
    homepage.raise_for_status()
    page = homepage.text

    csrf_match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page)
    if not csrf_match:
        raise RuntimeError("Could not find csrf token on Inovalink homepage.")
    csrf_token = csrf_match.group(1)

    snapshot_match = re.search(
        r'wire:snapshot="([^"]+)"[^>]*wire:id="([^"]+)"[^>]*wire:init="reloadData\(\)"',
        page,
    )
    if not snapshot_match:
        raise RuntimeError("Could not find the Livewire site.map component.")

    initial_snapshot = html.unescape(snapshot_match.group(1))
    component_id = snapshot_match.group(2)
    snapshot_obj = json.loads(initial_snapshot)
    component_name = snapshot_obj.get("memo", {}).get("name", "site.map")

    request_payload = {
        "_token": csrf_token,
        "components": [
            {
                "snapshot": initial_snapshot,
                "updates": {},
                "calls": [{"path": "", "method": "reloadData", "params": []}],
            }
        ],
    }

    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": ENTRYPOINT_URL.rstrip("/"),
        "Referer": ENTRYPOINT_URL,
        "X-CSRF-TOKEN": csrf_token,
        "X-Livewire": "true",
    }

    response = session.post(
        LIVEWIRE_UPDATE_URL,
        json=request_payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    response_json = response.json()
    if redact_google_api_keys:
        response_json = _redact_google_api_keys(response_json)

    actors = _extract_livewire_actor_payload(response_json)
    response_bytes = response.content
    response_checksum = _sha256_bytes(response_bytes)

    components = response_json.get("components") or []
    component = components[0]
    component_snapshot = component.get("snapshot")
    if isinstance(component_snapshot, str):
        component_snapshot = json.loads(component_snapshot)
    component_effects = component.get("effects") or {}
    component_html = component_effects.get("html")

    collection_counts = {
        name: len(values)
        for name, values in actors.items()
        if isinstance(values, list)
    }
    records_total = sum(collection_counts.values())

    artifact_dir = _ensure_artifact_dir(collected_at)
    prefix = f"inovalink_livewire_{collected_at.strftime('%Y%m%dT%H%M%SZ')}"
    request_path = artifact_dir / f"{prefix}_request.json"
    response_path = artifact_dir / f"{prefix}_response.json"

    request_record = {
        "entrypoint_url": ENTRYPOINT_URL,
        "request_url": LIVEWIRE_UPDATE_URL,
        "headers": headers,
        "payload": request_payload,
    }
    request_json = json.dumps(request_record, ensure_ascii=False, indent=2).encode("utf-8")
    response_json_bytes = json.dumps(response_json, ensure_ascii=False, indent=2).encode("utf-8")
    request_path.write_bytes(request_json)
    response_path.write_bytes(response_json_bytes)

    request_uri = _upload_artifact_to_minio(
        collected_at,
        request_path.name,
        request_json,
        {
            "kind": "request",
            "component-name": component_name,
            "collected-at": collected_at.isoformat(),
        },
    )
    response_uri = _upload_artifact_to_minio(
        collected_at,
        response_path.name,
        response_json_bytes,
        {
            "kind": "response",
            "component-name": component_name,
            "sha256": response_checksum,
            "collected-at": collected_at.isoformat(),
        },
    )

    return {
        "entrypoint_url": ENTRYPOINT_URL,
        "request_url": LIVEWIRE_UPDATE_URL,
        "request_method": "POST",
        "component_name": component_name,
        "component_id": component_id,
        "collected_at": collected_at.isoformat(),
        "snapshot_date": None,
        "response_status_code": response.status_code,
        "response_content_type": response.headers.get("Content-Type"),
        "response_content_encoding": response.headers.get("Content-Encoding"),
        "response_size_bytes": len(response_bytes),
        "response_checksum_sha256": response_checksum,
        "http_etag": response.headers.get("ETag"),
        "http_last_modified": response.headers.get("Last-Modified"),
        "collection_counts": collection_counts,
        "records_total": records_total,
        "raw_request_path": request_uri,
        "raw_response_path": response_uri,
        "local_request_path": str(request_path),
        "local_response_path": str(response_path),
        "component_snapshot": component_snapshot,
        "component_effects": component_effects,
        "component_html": component_html,
        "actors": actors,
    }


def register_sync_manifest(collection_payload: dict[str, Any]) -> dict[str, Any]:
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inovalink.sync_manifest (
                    entrypoint_url,
                    request_url,
                    endpoint_path,
                    request_method,
                    component_name,
                    component_id,
                    collected_at,
                    snapshot_date,
                    response_status_code,
                    response_content_type,
                    response_content_encoding,
                    response_size_bytes,
                    response_checksum_sha256,
                    http_etag,
                    http_last_modified,
                    collection_counts,
                    records_total,
                    raw_request_path,
                    raw_response_path,
                    processing_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 'collected'
                )
                RETURNING id, processing_status, collected_at
                """,
                (
                    collection_payload["entrypoint_url"],
                    collection_payload["request_url"],
                    "/livewire/update",
                    collection_payload["request_method"],
                    collection_payload["component_name"],
                    collection_payload["component_id"],
                    _parse_timestamp(collection_payload["collected_at"]),
                    collection_payload.get("snapshot_date"),
                    collection_payload.get("response_status_code"),
                    collection_payload.get("response_content_type"),
                    collection_payload.get("response_content_encoding"),
                    collection_payload.get("response_size_bytes"),
                    collection_payload.get("response_checksum_sha256"),
                    collection_payload.get("http_etag"),
                    _parse_timestamp(collection_payload.get("http_last_modified")),
                    psycopg2.extras.Json(collection_payload.get("collection_counts") or {}),
                    collection_payload.get("records_total"),
                    collection_payload.get("raw_request_path"),
                    collection_payload.get("raw_response_path"),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {
            "manifest_id": row[0],
            "processing_status": row[1],
            "collected_at": row[2].isoformat() if row and row[2] else None,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def store_component_payload(manifest_id: int, collection_payload: dict[str, Any]) -> dict[str, Any]:
    actors = collection_payload.get("actors") or {}
    payload_row_count = sum(len(values) for values in actors.values() if isinstance(values, list))
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inovalink.component_payload (
                    manifest_id,
                    component_name,
                    component_id,
                    component_snapshot,
                    component_effects,
                    component_html,
                    payload_row_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (manifest_id, component_name, component_id)
                DO UPDATE SET
                    component_snapshot = EXCLUDED.component_snapshot,
                    component_effects = EXCLUDED.component_effects,
                    component_html = EXCLUDED.component_html,
                    payload_row_count = EXCLUDED.payload_row_count
                """,
                (
                    manifest_id,
                    collection_payload["component_name"],
                    collection_payload["component_id"],
                    psycopg2.extras.Json(collection_payload.get("component_snapshot") or {}),
                    psycopg2.extras.Json(collection_payload.get("component_effects") or {}),
                    collection_payload.get("component_html"),
                    payload_row_count,
                ),
            )
        conn.commit()
        return {
            "manifest_id": manifest_id,
            "component_name": collection_payload["component_name"],
            "payload_row_count": payload_row_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _build_organization_rows(manifest_id: int, actors: dict[str, list[dict[str, Any]]]) -> tuple[list[tuple[Any, ...]], dict[str, int]]:
    rows: list[tuple[Any, ...]] = []
    counts: dict[str, int] = {}
    for source_collection, items in actors.items():
        if source_collection not in COLLECTION_TO_ENTITY_KIND:
            continue
        counts[source_collection] = len(items)
        for actor in items:
            address = actor.get("address") or {}
            city = address.get("city") or {}
            state = city.get("state") or {}
            rows.append(
                (
                    manifest_id,
                    source_collection,
                    actor.get("id"),
                    COLLECTION_TO_ENTITY_KIND[source_collection],
                    actor.get("category_id"),
                    COLLECTION_TO_CATEGORY_LABEL.get(source_collection),
                    actor.get("linkable_type"),
                    actor.get("linkable_id"),
                    _normalize_text(actor.get("name")) or f"source-record-{actor.get('id')}",
                    _normalize_text(actor.get("acronym")),
                    _normalize_text(actor.get("corporate_name")),
                    _normalize_digits(actor.get("cnpj"), 14),
                    _normalize_text(actor.get("telephone")),
                    _normalize_text(actor.get("email")),
                    _normalize_text(actor.get("site")),
                    _normalize_text(actor.get("instagram")),
                    _normalize_text(actor.get("linkedin")),
                    _normalize_text(actor.get("logo_path")),
                    _normalize_text(actor.get("institution")),
                    _to_bool(actor.get("active")),
                    _to_int(actor.get("company_status_id")),
                    _to_int(actor.get("cnae_id")),
                    _to_bool(actor.get("graduated")),
                    _to_int(actor.get("founding_year")),
                    _parse_date(actor.get("founding_date")),
                    _to_int(actor.get("termination_year")),
                    _to_int(actor.get("pre_acceleration_year")),
                    _to_int(actor.get("acceleration_year")),
                    _to_int(actor.get("pre_incubation_year")),
                    _to_int(actor.get("start_incubation_year")),
                    _to_int(actor.get("end_incubation_year")),
                    _to_bool(actor.get("spin_off")),
                    [_normalize_text(v) for v in (actor.get("expertise_areas") or []) if _normalize_text(v)],
                    psycopg2.extras.Json(actor.get("affiliated_societies_cities") or []),
                    psycopg2.extras.Json(actor.get("motivations_for_opening") or []),
                    psycopg2.extras.Json(actor.get("main_participation_aspects") or []),
                    _to_int(actor.get("meet_way_id")),
                    _normalize_digits(address.get("postcode"), 8),
                    _normalize_text(address.get("street")),
                    _normalize_text(address.get("number")),
                    _normalize_text(address.get("complement")),
                    _normalize_text(address.get("neighborhood")),
                    _normalize_digits(city.get("id"), 7),
                    _normalize_text(city.get("name")),
                    _normalize_digits(state.get("id"), 2),
                    _normalize_text(state.get("name")),
                    _normalize_text(state.get("uf")),
                    _to_decimal(address.get("latitude")),
                    _to_decimal(address.get("longitude")),
                    _parse_timestamp(actor.get("created_at")),
                    _parse_timestamp(actor.get("updated_at")),
                    psycopg2.extras.Json(actor),
                )
            )
    return rows, counts


def load_organization_snapshot(manifest_id: int, actors: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    rows, counts = _build_organization_rows(manifest_id, actors)
    if not rows:
        return {"manifest_id": manifest_id, "rows_inserted": 0, "rows_by_collection": counts}

    conn = _get_db_connection()
    sql = """
        INSERT INTO inovalink.organization_snapshot (
            manifest_id, source_collection, source_record_id, entity_kind,
            category_id, category_label, source_linkable_type, source_linkable_id,
            name, acronym, corporate_name, cnpj, telephone, email, site,
            instagram, linkedin, logo_path, institution, active, company_status_id,
            cnae_id, graduated, founding_year, founding_date, termination_year,
            pre_acceleration_year, acceleration_year, pre_incubation_year,
            start_incubation_year, end_incubation_year, spin_off,
            expertise_area_codes, affiliated_societies_cities,
            motivations_for_opening, main_participation_aspects, meet_way_id,
            postcode, street, street_number, complement, neighborhood,
            city_ibge_code, city_name, state_ibge_code, state_name, state_abbrev,
            latitude, longitude, source_created_at, source_updated_at, raw_payload
        ) VALUES %s
        ON CONFLICT (manifest_id, source_collection, source_record_id)
        DO UPDATE SET
            entity_kind = EXCLUDED.entity_kind,
            category_id = EXCLUDED.category_id,
            category_label = EXCLUDED.category_label,
            source_linkable_type = EXCLUDED.source_linkable_type,
            source_linkable_id = EXCLUDED.source_linkable_id,
            name = EXCLUDED.name,
            acronym = EXCLUDED.acronym,
            corporate_name = EXCLUDED.corporate_name,
            cnpj = EXCLUDED.cnpj,
            telephone = EXCLUDED.telephone,
            email = EXCLUDED.email,
            site = EXCLUDED.site,
            instagram = EXCLUDED.instagram,
            linkedin = EXCLUDED.linkedin,
            logo_path = EXCLUDED.logo_path,
            institution = EXCLUDED.institution,
            active = EXCLUDED.active,
            company_status_id = EXCLUDED.company_status_id,
            cnae_id = EXCLUDED.cnae_id,
            graduated = EXCLUDED.graduated,
            founding_year = EXCLUDED.founding_year,
            founding_date = EXCLUDED.founding_date,
            termination_year = EXCLUDED.termination_year,
            pre_acceleration_year = EXCLUDED.pre_acceleration_year,
            acceleration_year = EXCLUDED.acceleration_year,
            pre_incubation_year = EXCLUDED.pre_incubation_year,
            start_incubation_year = EXCLUDED.start_incubation_year,
            end_incubation_year = EXCLUDED.end_incubation_year,
            spin_off = EXCLUDED.spin_off,
            expertise_area_codes = EXCLUDED.expertise_area_codes,
            affiliated_societies_cities = EXCLUDED.affiliated_societies_cities,
            motivations_for_opening = EXCLUDED.motivations_for_opening,
            main_participation_aspects = EXCLUDED.main_participation_aspects,
            meet_way_id = EXCLUDED.meet_way_id,
            postcode = EXCLUDED.postcode,
            street = EXCLUDED.street,
            street_number = EXCLUDED.street_number,
            complement = EXCLUDED.complement,
            neighborhood = EXCLUDED.neighborhood,
            city_ibge_code = EXCLUDED.city_ibge_code,
            city_name = EXCLUDED.city_name,
            state_ibge_code = EXCLUDED.state_ibge_code,
            state_name = EXCLUDED.state_name,
            state_abbrev = EXCLUDED.state_abbrev,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            source_created_at = EXCLUDED.source_created_at,
            source_updated_at = EXCLUDED.source_updated_at,
            raw_payload = EXCLUDED.raw_payload
    """
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()
        return {
            "manifest_id": manifest_id,
            "rows_inserted": len(rows),
            "rows_by_collection": counts,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _derive_parent_collections(model_class: str) -> list[str]:
    if model_class == "App\\Models\\Incubator":
        return ["incubators"]
    if model_class == "App\\Models\\Accelerator":
        return ["accelerators"]
    if model_class == "App\\Models\\Park":
        return ["operationParks", "implantationParks", "planningParks"]
    return []


def load_organization_relationship(manifest_id: int) -> dict[str, Any]:
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, source_collection, source_record_id,
                       source_linkable_type, source_linkable_id
                FROM inovalink.organization_snapshot
                WHERE manifest_id = %s
                """,
                (manifest_id,),
            )
            rows = cur.fetchall()

        lookup = {(row["source_collection"], row["source_record_id"]): row["id"] for row in rows}
        relationship_rows: list[tuple[Any, ...]] = []
        unresolved = 0

        for row in rows:
            linkable_type = row["source_linkable_type"]
            linkable_id = row["source_linkable_id"]
            if not linkable_type or linkable_id is None:
                continue

            parent_org_id = None
            for parent_collection in _derive_parent_collections(linkable_type):
                parent_org_id = lookup.get((parent_collection, linkable_id))
                if parent_org_id is not None:
                    break

            relationship_type = CHILD_COLLECTION_TO_RELATIONSHIP.get(
                row["source_collection"], "linked_to"
            )
            if parent_org_id is None:
                unresolved += 1

            relationship_rows.append(
                (
                    manifest_id,
                    row["id"],
                    relationship_type,
                    linkable_type,
                    linkable_id,
                    parent_org_id,
                    psycopg2.extras.Json(
                        {
                            "child_source_collection": row["source_collection"],
                            "linkable_type": linkable_type,
                            "linkable_id": linkable_id,
                        }
                    ),
                )
            )

        if not relationship_rows:
            return {"manifest_id": manifest_id, "rows_inserted": 0, "rows_unresolved": 0}

        sql = """
            INSERT INTO inovalink.organization_relationship (
                manifest_id,
                child_organization_id,
                relationship_type,
                parent_model_class,
                parent_source_record_id,
                parent_organization_id,
                relationship_metadata
            ) VALUES %s
            ON CONFLICT (
                manifest_id,
                child_organization_id,
                relationship_type,
                parent_model_class,
                parent_source_record_id
            )
            DO UPDATE SET
                parent_organization_id = EXCLUDED.parent_organization_id,
                relationship_metadata = EXCLUDED.relationship_metadata
        """
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, relationship_rows, page_size=500)
        conn.commit()
        return {
            "manifest_id": manifest_id,
            "rows_inserted": len(relationship_rows),
            "rows_unresolved": unresolved,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def seed_organization_cnpj_match(manifest_id: int) -> dict[str, Any]:
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, cnpj
                FROM inovalink.organization_snapshot
                WHERE manifest_id = %s
                """,
                (manifest_id,),
            )
            rows = cur.fetchall()

        match_rows: list[tuple[Any, ...]] = []
        rows_with_source_cnpj = 0
        for row in rows:
            raw_cnpj = _normalize_text(row["cnpj"])
            normalized = _normalize_digits(raw_cnpj, 14) if raw_cnpj else None
            if raw_cnpj:
                rows_with_source_cnpj += 1
            if raw_cnpj and normalized is None:
                status = "invalid"
                cnpj_basico = None
                cnpj_ordem = None
                cnpj_dv = None
            else:
                status = "pending"
                cnpj_basico = normalized[:8] if normalized else None
                cnpj_ordem = normalized[8:12] if normalized else None
                cnpj_dv = normalized[12:14] if normalized else None

            match_rows.append(
                (
                    row["id"],
                    status,
                    raw_cnpj,
                    cnpj_basico,
                    cnpj_ordem,
                    cnpj_dv,
                    normalized,
                )
            )

        if not match_rows:
            return {
                "manifest_id": manifest_id,
                "rows_seeded": 0,
                "rows_with_source_cnpj": 0,
            }

        sql = """
            INSERT INTO inovalink.organization_cnpj_match (
                organization_snapshot_id,
                match_status,
                source_cnpj,
                cnpj_basico,
                cnpj_ordem,
                cnpj_dv,
                cnpj_14
            ) VALUES %s
            ON CONFLICT (organization_snapshot_id)
            DO UPDATE SET
                source_cnpj = EXCLUDED.source_cnpj,
                cnpj_basico = EXCLUDED.cnpj_basico,
                cnpj_ordem = EXCLUDED.cnpj_ordem,
                cnpj_dv = EXCLUDED.cnpj_dv,
                cnpj_14 = EXCLUDED.cnpj_14,
                match_status = CASE
                    WHEN inovalink.organization_cnpj_match.match_status = 'matched'
                        THEN inovalink.organization_cnpj_match.match_status
                    WHEN inovalink.organization_cnpj_match.match_status = 'ambiguous'
                        THEN inovalink.organization_cnpj_match.match_status
                    WHEN inovalink.organization_cnpj_match.match_status = 'unmatched'
                        THEN inovalink.organization_cnpj_match.match_status
                    ELSE EXCLUDED.match_status
                END
        """
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, match_rows, page_size=500)
        conn.commit()
        return {
            "manifest_id": manifest_id,
            "rows_seeded": len(match_rows),
            "rows_with_source_cnpj": rows_with_source_cnpj,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def resolve_exact_cnpj_matches(manifest_id: int) -> dict[str, Any]:
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    match.id,
                    match.organization_snapshot_id,
                    match.cnpj_basico,
                    match.cnpj_ordem,
                    match.cnpj_dv,
                    match.cnpj_14,
                    org.name,
                    est.cnpj_basico AS matched_estab_cnpj_basico,
                    est.cnpj_ordem AS matched_estab_cnpj_ordem,
                    est.cnpj_dv AS matched_estab_cnpj_dv,
                    emp.cnpj_basico AS matched_empresa_cnpj_basico
                FROM inovalink.organization_cnpj_match AS match
                JOIN inovalink.organization_snapshot AS org
                    ON org.id = match.organization_snapshot_id
                LEFT JOIN cnpj.estabelecimento AS est
                    ON est.cnpj_basico = match.cnpj_basico
                   AND est.cnpj_ordem = match.cnpj_ordem
                   AND est.cnpj_dv = match.cnpj_dv
                LEFT JOIN cnpj.empresa AS emp
                    ON emp.cnpj_basico = est.cnpj_basico
                WHERE org.manifest_id = %s
                  AND match.cnpj_14 IS NOT NULL
                  AND match.match_status IN ('pending', 'unmatched')
                """,
                (manifest_id,),
            )
            candidates = cur.fetchall()

        if not candidates:
            return {
                "manifest_id": manifest_id,
                "matched_rows": 0,
                "unmatched_rows": 0,
            }

        matched_rows: list[tuple[Any, ...]] = []
        unmatched_rows: list[int] = []
        for candidate in candidates:
            evidence = {
                "organization_name": candidate["name"],
                "cnpj_14": candidate["cnpj_14"],
            }
            if candidate["matched_estab_cnpj_basico"] is not None:
                matched_rows.append(
                    (
                        "matched",
                        "exact_cnpj_estabelecimento",
                        Decimal("1.0"),
                        datetime.now(timezone.utc),
                        candidate["matched_empresa_cnpj_basico"],
                        candidate["matched_estab_cnpj_basico"],
                        candidate["matched_estab_cnpj_ordem"],
                        candidate["matched_estab_cnpj_dv"],
                        psycopg2.extras.Json(evidence),
                        candidate["id"],
                    )
                )
            else:
                unmatched_rows.append(candidate["id"])

        with conn.cursor() as cur:
            if matched_rows:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    UPDATE inovalink.organization_cnpj_match
                    SET match_status = %s,
                        match_method = %s,
                        match_confidence = %s,
                        matched_at = %s,
                        matched_empresa_cnpj_basico = %s,
                        matched_estab_cnpj_basico = %s,
                        matched_estab_cnpj_ordem = %s,
                        matched_estab_cnpj_dv = %s,
                        evidence = %s
                    WHERE id = %s
                    """,
                    matched_rows,
                    page_size=500,
                )
            if unmatched_rows:
                cur.execute(
                    """
                    UPDATE inovalink.organization_cnpj_match
                    SET match_status = 'unmatched',
                        match_method = 'exact_cnpj_estabelecimento',
                        match_confidence = NULL,
                        matched_at = NULL,
                        matched_empresa_cnpj_basico = NULL,
                        matched_estab_cnpj_basico = NULL,
                        matched_estab_cnpj_ordem = NULL,
                        matched_estab_cnpj_dv = NULL,
                        evidence = COALESCE(evidence, '{}'::jsonb) || jsonb_build_object('exact_match_attempted', true)
                    WHERE id = ANY(%s)
                    """,
                    (unmatched_rows,),
                )

        conn.commit()
        return {
            "manifest_id": manifest_id,
            "matched_rows": len(matched_rows),
            "unmatched_rows": len(unmatched_rows),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_manifest_loaded(manifest_id: int) -> dict[str, Any]:
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inovalink.sync_manifest
                SET processing_status = 'loaded',
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                RETURNING id, processing_status
                """,
                (manifest_id,),
            )
            row = cur.fetchone()
        conn.commit()
        return {"manifest_id": row[0], "processing_status": row[1]} if row else {}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_manifest_failed(manifest_id: int, error_message: str) -> None:
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inovalink.sync_manifest
                SET processing_status = 'failed',
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (error_message[:4000], manifest_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()