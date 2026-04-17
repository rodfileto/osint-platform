"""FINEP FastAPI router for relational project and release endpoints."""

from __future__ import annotations

import asyncio
import json
import unicodedata
from collections import defaultdict
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.schemas.finep import (
    FinepBootstrapStatus,
    FinepCompanyLookup,
    FinepGiniUfItem,
    FinepMapFeature,
    FinepMapMetadata,
    FinepMunicipioMapaResponse,
    FinepRelatedProject,
    FinepResumoCnaeItem,
    FinepResumoEmpresaItem,
    FinepResumoGeralResponse,
    FinepResumoMunicipioItem,
    FinepResumoUfItem,
    FinepResourceRecord,
    PaginatedFinepResumoCnaeResponse,
    PaginatedFinepResumoEmpresaResponse,
    PaginatedFinepResumoMunicipioResponse,
    PaginatedFinepResumoUfResponse,
    PaginatedFinepResourceResponse,
)
from core.database import DatabaseManager, get_database_manager

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 100

RESOURCE_CONFIGS: dict[str, dict[str, Any]] = {
    "operacao-direta": {
        "table": "finep.projetos_operacao_direta",
        "id_field": "id",
        "order_by": "data_assinatura DESC NULLS LAST, contrato ASC NULLS LAST",
        "search_fields": ["titulo", "proponente", "executor", "demanda", "status", "instrumento"],
        "cnpj_field": "cnpj_proponente_norm",
        "join_fields": ["cnpj_proponente_norm", "cnpj_executor_norm"],
        "company_payloads": {
            "proponente_empresa": "cnpj_proponente_norm",
            "executor_empresa": "cnpj_executor_norm",
        },
    },
    "credito-descentralizado": {
        "table": "finep.projetos_credito_descentralizado",
        "id_field": "id",
        "order_by": "data_assinatura DESC NULLS LAST, contrato ASC NULLS LAST",
        "search_fields": ["proponente", "agente_financeiro"],
        "cnpj_field": "cnpj_proponente_norm",
        "join_fields": ["cnpj_proponente_norm"],
        "company_payloads": {
            "proponente_empresa": "cnpj_proponente_norm",
        },
    },
    "investimento": {
        "table": "finep.projetos_investimento",
        "id_field": "id",
        "order_by": "data_assinatura DESC NULLS LAST, numero_contrato ASC NULLS LAST",
        "search_fields": ["proponente", "numero_contrato", "ref"],
        "cnpj_field": "cnpj_proponente_norm",
        "join_fields": ["cnpj_proponente_norm"],
        "company_payloads": {
            "proponente_empresa": "cnpj_proponente_norm",
        },
    },
    "ancine": {
        "table": "finep.projetos_ancine",
        "id_field": "id",
        "order_by": "data_assinatura DESC NULLS LAST, contrato ASC NULLS LAST",
        "search_fields": ["titulo", "proponente", "executor", "demanda", "status", "instrumento"],
        "cnpj_field": "cnpj_proponente_norm",
        "join_fields": ["cnpj_proponente_norm"],
        "company_payloads": {
            "proponente_empresa": "cnpj_proponente_norm",
        },
    },
    "liberacoes-operacao-direta": {
        "table": "finep.liberacoes_operacao_direta",
        "id_field": "id",
        "order_by": "data_liberacao DESC NULLS LAST, contrato ASC",
        "search_fields": ["contrato", "ref"],
        "project_lookup": {
            "table": "finep.projetos_operacao_direta",
            "key_fields": ["contrato", "ref"],
            "value_fields": [
                "id",
                "contrato",
                "ref",
                "titulo",
                "proponente",
                "cnpj_proponente_norm",
                "executor",
                "status",
                "instrumento",
                "uf",
                "data_assinatura",
                "valor_finep",
                "valor_pago",
            ],
        },
    },
    "liberacoes-credito-descentralizado": {
        "table": "finep.liberacoes_credito_descentralizado",
        "id_field": "id",
        "order_by": "data_liberacao DESC NULLS LAST, contrato ASC",
        "search_fields": ["contrato"],
        "cnpj_field": "cnpj_proponente_norm",
        "join_fields": ["cnpj_proponente_norm"],
        "company_payloads": {
            "proponente_empresa": "cnpj_proponente_norm",
        },
    },
    "liberacoes-ancine": {
        "table": "finep.liberacoes_ancine",
        "id_field": "id",
        "order_by": "data_liberacao DESC NULLS LAST, contrato ASC",
        "search_fields": ["contrato", "ref"],
        "project_lookup": {
            "table": "finep.projetos_ancine",
            "key_fields": ["contrato", "ref"],
            "value_fields": [
                "id",
                "contrato",
                "ref",
                "titulo",
                "proponente",
                "cnpj_proponente_norm",
                "executor",
                "status",
                "instrumento",
                "uf",
                "data_assinatura",
                "valor_finep",
                "valor_pago",
            ],
        },
    },
}

IMPLEMENTED_ENDPOINTS = [
    "/operacao-direta",
    "/operacao-direta/{item_id}",
    "/credito-descentralizado",
    "/credito-descentralizado/{item_id}",
    "/investimento",
    "/investimento/{item_id}",
    "/ancine",
    "/ancine/{item_id}",
    "/liberacoes-operacao-direta",
    "/liberacoes-operacao-direta/{item_id}",
    "/liberacoes-credito-descentralizado",
    "/liberacoes-credito-descentralizado/{item_id}",
    "/liberacoes-ancine",
    "/liberacoes-ancine/{item_id}",
]

PENDING_ENDPOINTS = [
]

IMPLEMENTED_ENDPOINTS.extend(
    [
        "/resumo-geral",
        "/resumo-empresa",
        "/resumo-uf",
        "/resumo-cnae",
        "/resumo-municipio",
        "/mapa-municipio",
    ]
)


def _digits_only(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _decimal_or_zero(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalize_municipio(value: Any) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).strip().split())
    return normalized or None


def _municipio_match_key(value: Any) -> str | None:
    normalized = _normalize_municipio(value)
    if normalized is None:
        return None
    ascii_value = unicodedata.normalize("NFKD", normalized)
    ascii_value = "".join(character for character in ascii_value if not unicodedata.combining(character))
    return ascii_value.upper()


def _calculate_gini(values: list[Any]) -> float:
    non_negative_values = [float(value) for value in values if value is not None and float(value) >= 0]
    if not non_negative_values:
        return 0.0
    sorted_values = sorted(non_negative_values)
    total = sum(sorted_values)
    if total == 0:
        return 0.0
    weighted_sum = 0.0
    item_count = len(sorted_values)
    for index, value in enumerate(sorted_values, start=1):
        weighted_sum += ((2 * index) - item_count - 1) * value
    return round(weighted_sum / (item_count * total), 6)


def _safe_non_negative_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, 0.0)


def _build_page_url(request: Request, page: int, page_size: int) -> str:
    return str(request.url.include_query_params(page=page, page_size=page_size))


def _build_filters(
    config: dict[str, Any],
    q: str | None,
    contrato: str | None,
    ref: str | None,
    uf: str | None,
    cnpj: str | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}

    if q and config.get("search_fields"):
        text_clauses = []
        for index, field_name in enumerate(config["search_fields"]):
            key = f"q_{index}"
            text_clauses.append(f"CAST({field_name} AS TEXT) ILIKE :{key}")
            params[key] = f"%{q.strip()}%"
        clauses.append(f"({' OR '.join(text_clauses)})")

    if contrato:
        clauses.append("contrato ILIKE :contrato")
        params["contrato"] = f"%{contrato.strip()}%"

    if ref and any(field == "ref" for field in config.get("search_fields", [])) or ref and "project_lookup" in config:
        if "ref" in config.get("search_fields", []) or config["table"].endswith("_direta") or config["table"].endswith("_ancine"):
            clauses.append("ref ILIKE :ref")
            params["ref"] = f"%{ref.strip()}%"

    if uf and "uf" in str(config["table"]):
        clauses.append("uf = :uf")
        params["uf"] = uf.strip().upper()
    elif uf and config["table"] in {"finep.projetos_operacao_direta", "finep.projetos_credito_descentralizado", "finep.projetos_ancine"}:
        clauses.append("uf = :uf")
        params["uf"] = uf.strip().upper()

    if cnpj and config.get("cnpj_field"):
        lookup = config["cnpj_field"]
        digits = _digits_only(cnpj)
        if digits:
            if len(digits) == 14:
                clauses.append(f"{lookup} = :cnpj")
                params["cnpj"] = digits
            else:
                clauses.append(f"{lookup} LIKE :cnpj_prefix")
                params["cnpj_prefix"] = f"{digits}%"

    if not clauses:
        return "", params
    return f"WHERE {' AND '.join(clauses)}", params


async def _fetch_company_lookup(database: DatabaseManager, cnpjs: set[str]) -> dict[str, FinepCompanyLookup]:
    if not cnpjs:
        return {}

    active_rows = await database.fetch_all(
        """
        SELECT
            cnpj_14,
            razao_social,
            nome_fantasia,
            situacao_cadastral,
            municipio_nome,
            uf,
            cnae_fiscal_principal,
            cnae_descricao,
            porte_empresa,
            natureza_juridica,
            natureza_juridica_descricao,
            capital_social,
            data_inicio_atividade,
            reference_month,
            'active' AS match_source
        FROM cnpj.mv_company_search
        WHERE cnpj_14 = ANY(:cnpjs)
        """,
        {"cnpjs": list(cnpjs)},
    )
    lookup = {row["cnpj_14"]: FinepCompanyLookup.model_validate(row) for row in active_rows}

    missing = sorted(cnpjs.difference(lookup.keys()))
    if missing:
        inactive_rows = await database.fetch_all(
            """
            SELECT
                cnpj_14,
                razao_social,
                nome_fantasia,
                situacao_cadastral,
                municipio_nome,
                uf,
                cnae_fiscal_principal,
                cnae_descricao,
                porte_empresa,
                natureza_juridica,
                natureza_juridica_descricao,
                capital_social,
                data_inicio_atividade,
                reference_month,
                'inactive' AS match_source
            FROM cnpj.mv_company_search_inactive
            WHERE cnpj_14 = ANY(:cnpjs)
            """,
            {"cnpjs": missing},
        )
        lookup.update({row["cnpj_14"]: FinepCompanyLookup.model_validate(row) for row in inactive_rows})

    return lookup


async def _collect_project_records(database: DatabaseManager) -> list[dict[str, Any]]:
    operacao_rows, credito_rows, investimento_rows, ancine_rows = await asyncio.gather(
        database.fetch_all(
            """
            SELECT id, cnpj_proponente_norm, municipio, uf, valor_finep, valor_pago, instrumento
            FROM finep.projetos_operacao_direta
            """
        ),
        database.fetch_all(
            """
            SELECT id, cnpj_proponente_norm, uf, valor_financiado, valor_liberado
            FROM finep.projetos_credito_descentralizado
            """
        ),
        database.fetch_all(
            """
            SELECT id, cnpj_proponente_norm, valor_total_contratado, valor_total_liberado
            FROM finep.projetos_investimento
            """
        ),
        database.fetch_all(
            """
            SELECT id, cnpj_proponente_norm, municipio, uf, valor_finep, valor_pago, instrumento
            FROM finep.projetos_ancine
            """
        ),
    )

    records: list[dict[str, Any]] = []
    for row in operacao_rows:
        records.append(
            {
                "source": "operacao_direta",
                "source_id": row["id"],
                "cnpj_14": row["cnpj_proponente_norm"],
                "municipio_nome": _normalize_municipio(row["municipio"]),
                "uf": row["uf"],
                "total_aprovado_finep": _decimal_or_zero(row["valor_finep"]),
                "total_liberado": _decimal_or_zero(row["valor_pago"]),
                "instrumento": row["instrumento"],
            }
        )
    for row in credito_rows:
        records.append(
            {
                "source": "credito_descentralizado",
                "source_id": row["id"],
                "cnpj_14": row["cnpj_proponente_norm"],
                "municipio_nome": None,
                "uf": row["uf"],
                "total_aprovado_finep": _decimal_or_zero(row["valor_financiado"]),
                "total_liberado": _decimal_or_zero(row["valor_liberado"]),
                "instrumento": "Credito Descentralizado",
            }
        )
    for row in investimento_rows:
        records.append(
            {
                "source": "investimento",
                "source_id": row["id"],
                "cnpj_14": row["cnpj_proponente_norm"],
                "municipio_nome": None,
                "uf": None,
                "total_aprovado_finep": _decimal_or_zero(row["valor_total_contratado"]),
                "total_liberado": _decimal_or_zero(row["valor_total_liberado"]),
                "instrumento": "Investimento",
            }
        )
    for row in ancine_rows:
        records.append(
            {
                "source": "ancine",
                "source_id": row["id"],
                "cnpj_14": row["cnpj_proponente_norm"],
                "municipio_nome": _normalize_municipio(row["municipio"]),
                "uf": row["uf"],
                "total_aprovado_finep": _decimal_or_zero(row["valor_finep"]),
                "total_liberado": _decimal_or_zero(row["valor_pago"]),
                "instrumento": row["instrumento"] or "Ancine",
            }
        )

    company_cnpjs = {
        record["cnpj_14"] for record in records if record.get("cnpj_14") and len(str(record["cnpj_14"])) == 14
    }
    companies = await _fetch_company_lookup(database, company_cnpjs)
    for record in records:
        company = companies.get(record.get("cnpj_14"))
        record["company"] = company.model_dump() if company is not None else None
        if company:
            if not record.get("uf"):
                record["uf"] = company.uf
            if not record.get("municipio_nome"):
                record["municipio_nome"] = _normalize_municipio(company.municipio_nome)
    return records


def _filter_analytics_records(records: list[dict[str, Any]], cnpj: str | None, uf: str | None, q: str | None) -> list[dict[str, Any]]:
    digits = _digits_only(cnpj or "")
    normalized_q = (q or "").strip().lower()
    normalized_uf = (uf or "").strip().upper()
    filtered: list[dict[str, Any]] = []
    for record in records:
        company = record.get("company") or {}
        if digits:
            record_cnpj = record.get("cnpj_14") or ""
            if len(digits) == 14:
                if record_cnpj != digits:
                    continue
            elif not str(record_cnpj).startswith(digits):
                continue
        if normalized_uf and (record.get("uf") or "") != normalized_uf:
            continue
        if normalized_q:
            haystacks = [
                company.get("razao_social") or "",
                company.get("nome_fantasia") or "",
                company.get("cnae_descricao") or "",
                record.get("municipio_nome") or "",
                record.get("instrumento") or "",
            ]
            if not any(normalized_q in str(haystack).lower() for haystack in haystacks):
                continue
        filtered.append(record)
    return filtered


async def _load_geo_municipality_lookup(
    database: DatabaseManager,
    records: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    company_codes = {
        company["codigo_municipio"]
        for record in records
        for company in [record.get("company") or {}]
        if company.get("codigo_municipio")
    }
    ufs = {record.get("uf") for record in records if record.get("uf")}

    by_cnpj_code: dict[str, dict[str, Any]] = {}
    by_name: dict[tuple[str, str], dict[str, Any]] = {}

    if company_codes:
        rows = await database.fetch_all(
            """
            SELECT
                cnpj_municipality_code,
                municipality_ibge_code,
                municipality_name,
                state_abbrev,
                ST_Y(centroid::geometry) AS latitude,
                ST_X(centroid::geometry) AS longitude
            FROM geo.v_cnpj_br_municipality
            WHERE cnpj_municipality_code = ANY(:codes)
              AND municipality_ibge_code IS NOT NULL
              AND centroid IS NOT NULL
            """,
            {"codes": list(company_codes)},
        )
        for row in rows:
            by_cnpj_code[row["cnpj_municipality_code"]] = {
                "municipality_ibge_code": row["municipality_ibge_code"],
                "municipio_nome": row["municipality_name"],
                "uf": row["state_abbrev"],
                "latitude": float(row["latitude"]) if row["latitude"] is not None else None,
                "longitude": float(row["longitude"]) if row["longitude"] is not None else None,
            }

    if ufs:
        rows = await database.fetch_all(
            """
            SELECT
                municipality_ibge_code,
                municipality_name,
                state_abbrev,
                ST_Y(centroid::geometry) AS latitude,
                ST_X(centroid::geometry) AS longitude
            FROM geo.br_municipality
            WHERE state_abbrev = ANY(:ufs)
              AND centroid IS NOT NULL
            """,
            {"ufs": list(ufs)},
        )
        for row in rows:
            key = (row["state_abbrev"], _municipio_match_key(row["municipality_name"]))
            by_name[key] = {
                "municipality_ibge_code": row["municipality_ibge_code"],
                "municipio_nome": row["municipality_name"],
                "uf": row["state_abbrev"],
                "latitude": float(row["latitude"]) if row["latitude"] is not None else None,
                "longitude": float(row["longitude"]) if row["longitude"] is not None else None,
            }

    return by_cnpj_code, by_name


def _resolve_geo_municipality(
    record: dict[str, Any],
    by_cnpj_code: dict[str, dict[str, Any]],
    by_name: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any] | None:
    company = record.get("company") or {}
    company_code = company.get("codigo_municipio")
    if company_code and company_code in by_cnpj_code:
        return by_cnpj_code[company_code]
    uf = record.get("uf")
    municipio_key = _municipio_match_key(record.get("municipio_nome"))
    if uf and municipio_key:
        return by_name.get((uf, municipio_key))
    return None


async def _build_municipio_summary(database: DatabaseManager, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str | None, str], dict[str, Any]] = {}
    geo_by_cnpj_code, geo_by_name = await _load_geo_municipality_lookup(database, records)
    for record in records:
        municipio_nome = _normalize_municipio(record.get("municipio_nome"))
        if not municipio_nome:
            continue
        uf = record.get("uf") or None
        group_key = (uf, municipio_nome.upper())
        geo_municipality = _resolve_geo_municipality(record, geo_by_cnpj_code, geo_by_name)
        resolved_name = geo_municipality["municipio_nome"] if geo_municipality else municipio_nome
        if group_key not in groups:
            groups[group_key] = {
                "municipio_nome": resolved_name,
                "uf": geo_municipality["uf"] if geo_municipality else uf,
                "municipality_ibge_code": geo_municipality["municipality_ibge_code"] if geo_municipality else None,
                "latitude": geo_municipality["latitude"] if geo_municipality else None,
                "longitude": geo_municipality["longitude"] if geo_municipality else None,
                "total_empresas": set(),
                "total_projetos": 0,
                "total_aprovado_finep": Decimal("0.00"),
                "total_liberado": Decimal("0.00"),
                "fontes": set(),
            }
        group = groups[group_key]
        if geo_municipality and not group.get("municipality_ibge_code"):
            group.update(
                {
                    "municipio_nome": geo_municipality["municipio_nome"],
                    "uf": geo_municipality["uf"],
                    "municipality_ibge_code": geo_municipality["municipality_ibge_code"],
                    "latitude": geo_municipality["latitude"],
                    "longitude": geo_municipality["longitude"],
                }
            )
        if record.get("cnpj_14"):
            group["total_empresas"].add(record["cnpj_14"])
        group["total_projetos"] += 1
        group["total_aprovado_finep"] += record["total_aprovado_finep"]
        group["total_liberado"] += record["total_liberado"]
        group["fontes"].add(record["source"])
    payload: list[dict[str, Any]] = []
    for row in groups.values():
        payload.append(
            {
                "municipio_nome": row["municipio_nome"],
                "uf": row["uf"],
                "municipality_ibge_code": row["municipality_ibge_code"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "total_empresas": len(row["total_empresas"]),
                "total_projetos": row["total_projetos"],
                "total_aprovado_finep": row["total_aprovado_finep"],
                "total_liberado": row["total_liberado"],
                "fontes": sorted(row["fontes"]),
            }
        )
    payload.sort(key=lambda item: (item["total_aprovado_finep"], item["total_projetos"]), reverse=True)
    return payload


async def _load_geo_municipality_features(
    database: DatabaseManager,
    municipio_summary: list[dict[str, Any]],
    simplify_tolerance: float = 0.0,
    uf: str | None = None,
) -> list[dict[str, Any]]:
    numeric_tolerance = max(float(simplify_tolerance or 0.0), 0.0)
    summary_by_code = {
        item["municipality_ibge_code"]: item
        for item in municipio_summary
        if item.get("municipality_ibge_code")
    }

    query = """
        SELECT
            municipality_ibge_code,
            municipality_name,
            state_abbrev,
            ST_Y(centroid::geometry) AS latitude,
            ST_X(centroid::geometry) AS longitude,
            ST_AsGeoJSON(
                ST_Transform(
                    CASE
                        WHEN :tolerance > 0 THEN ST_SimplifyPreserveTopology(geom, :tolerance)
                        ELSE geom
                    END,
                    4326
                ),
                5
            )::jsonb AS geometry
        FROM geo.br_municipality
    """
    params: dict[str, Any] = {"tolerance": numeric_tolerance}
    if uf:
        query += " WHERE state_abbrev = :uf"
        params["uf"] = uf
    query += " ORDER BY municipality_name"

    rows = await database.fetch_all(query, params)
    features: list[dict[str, Any]] = []
    for row in rows:
        geometry = row["geometry"]
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        item = summary_by_code.get(row["municipality_ibge_code"])
        approved_value = float(item["total_aprovado_finep"]) if item else 0.0
        released_value = float(item["total_liberado"]) if item else 0.0
        features.append(
            {
                "type": "Feature",
                "id": row["municipality_ibge_code"],
                "geometry": geometry,
                "properties": {
                    "municipality_ibge_code": row["municipality_ibge_code"],
                    "municipio_nome": row["municipality_name"],
                    "uf": row["state_abbrev"],
                    "latitude": item.get("latitude") if item else (float(row["latitude"]) if row["latitude"] is not None else None),
                    "longitude": item.get("longitude") if item else (float(row["longitude"]) if row["longitude"] is not None else None),
                    "total_empresas": item["total_empresas"] if item else 0,
                    "total_projetos": item["total_projetos"] if item else 0,
                    "total_aprovado_finep": approved_value,
                    "total_liberado": released_value,
                    "fontes": item["fontes"] if item else [],
                },
            }
        )
    return features


def _paginate_items(
    request: Request,
    items: list[Any],
    page: int,
    page_size: int,
) -> tuple[list[Any], int, str | None, str | None]:
    total = len(items)
    offset = (page - 1) * page_size
    paged = items[offset : offset + page_size]
    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return paged, total, next_url, previous_url


async def _fetch_project_lookup(
    database: DatabaseManager,
    project_config: dict[str, Any],
    keys: set[tuple[Any, ...]],
) -> dict[tuple[Any, ...], FinepRelatedProject]:
    if not keys:
        return {}

    contracts = sorted({key[0] for key in keys if key and key[0]})
    if not contracts:
        return {}

    value_fields = ",\n                ".join(project_config["value_fields"])
    rows = await database.fetch_all(
        f"""
        SELECT
            {value_fields}
        FROM {project_config['table']}
        WHERE contrato = ANY(:contracts)
        """,
        {"contracts": contracts},
    )
    cnpjs = {
        row.get("cnpj_proponente_norm")
        for row in rows
        if row.get("cnpj_proponente_norm") and len(str(row.get("cnpj_proponente_norm"))) == 14
    }
    company_lookup = await _fetch_company_lookup(database, cnpjs)

    lookup: dict[tuple[Any, ...], FinepRelatedProject] = {}
    for row in rows:
        row["proponente_empresa"] = company_lookup.get(row.get("cnpj_proponente_norm"))
        key = tuple(row.get(field) for field in project_config["key_fields"])
        lookup[key] = FinepRelatedProject.model_validate(row)
    return lookup


async def _enrich_rows(
    database: DatabaseManager,
    config: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[FinepResourceRecord]:
    join_fields = config.get("join_fields", [])
    cnpjs = {
        row.get(field)
        for row in rows
        for field in join_fields
        if row.get(field) and len(str(row.get(field))) == 14
    }
    company_lookup = await _fetch_company_lookup(database, cnpjs)

    project_lookup: dict[tuple[Any, ...], FinepRelatedProject] = {}
    if config.get("project_lookup"):
        keys = {
            tuple(row.get(field) for field in config["project_lookup"]["key_fields"])
            for row in rows
        }
        project_lookup = await _fetch_project_lookup(database, config["project_lookup"], keys)

    payloads: list[FinepResourceRecord] = []
    for row in rows:
        item = dict(row)
        for payload_name, field_name in config.get("company_payloads", {}).items():
            item[payload_name] = company_lookup.get(item.get(field_name))
        if config.get("project_lookup"):
            key = tuple(item.get(field) for field in config["project_lookup"]["key_fields"])
            item["projeto"] = project_lookup.get(key)
        payloads.append(FinepResourceRecord.model_validate(item))
    return payloads


async def _list_resource(
    database: DatabaseManager,
    request: Request,
    resource_key: str,
    q: str | None,
    contrato: str | None,
    ref: str | None,
    uf: str | None,
    cnpj: str | None,
    page: int,
    page_size: int,
) -> PaginatedFinepResourceResponse:
    config = RESOURCE_CONFIGS[resource_key]
    where_clause, params = _build_filters(config, q, contrato, ref, uf, cnpj)
    offset = (page - 1) * page_size

    total_row = await database.fetch_one(
        f"SELECT COUNT(*) AS total FROM {config['table']} {where_clause}",
        params,
    )
    total = int((total_row or {}).get("total", 0))

    rows = await database.fetch_all(
        f"SELECT * FROM {config['table']} {where_clause} ORDER BY {config['order_by']} LIMIT :limit OFFSET :offset",
        {**params, "limit": page_size, "offset": offset},
    )
    results = await _enrich_rows(database, config, rows)

    next_url = None
    previous_url = None
    if offset + page_size < total:
        next_url = _build_page_url(request, page + 1, page_size)
    if page > 1:
        previous_url = _build_page_url(request, page - 1, page_size)

    return PaginatedFinepResourceResponse(
        count=total,
        next=next_url,
        previous=previous_url,
        results=results,
    )


async def _get_resource(database: DatabaseManager, resource_key: str, item_id: int) -> FinepResourceRecord:
    config = RESOURCE_CONFIGS[resource_key]
    row = await database.fetch_one(
        f"SELECT * FROM {config['table']} WHERE {config['id_field']} = :item_id LIMIT 1",
        {"item_id": item_id},
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Registro não encontrado.")

    payloads = await _enrich_rows(database, config, [row])
    return payloads[0]


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", response_model=FinepBootstrapStatus, summary="FINEP migration status")
    async def finep_bootstrap_status() -> FinepBootstrapStatus:
        return FinepBootstrapStatus(
            domain="finep",
            status="partial",
            implemented_endpoints=IMPLEMENTED_ENDPOINTS,
            pending_endpoints=PENDING_ENDPOINTS,
        )

    def register_resource(resource_key: str, path: str, summary_list: str, summary_detail: str) -> None:
        @router.get(path, response_model=PaginatedFinepResourceResponse, summary=summary_list)
        async def list_resource(
            request: Request,
            q: str | None = Query(default=None),
            contrato: str | None = Query(default=None),
            ref: str | None = Query(default=None),
            uf: str | None = Query(default=None),
            cnpj: str | None = Query(default=None),
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
            database: DatabaseManager = Depends(get_database_manager),
            _resource_key: str = resource_key,
        ) -> PaginatedFinepResourceResponse:
            return await _list_resource(database, request, _resource_key, q, contrato, ref, uf, cnpj, page, page_size)

        @router.get(f"{path}/{{item_id}}", response_model=FinepResourceRecord, summary=summary_detail)
        async def get_resource(
            item_id: int,
            database: DatabaseManager = Depends(get_database_manager),
            _resource_key: str = resource_key,
        ) -> FinepResourceRecord:
            return await _get_resource(database, _resource_key, item_id)

    register_resource("operacao-direta", "/operacao-direta", "List direct operation projects", "Get direct operation project")
    register_resource(
        "credito-descentralizado",
        "/credito-descentralizado",
        "List decentralized credit projects",
        "Get decentralized credit project",
    )
    register_resource("investimento", "/investimento", "List investment projects", "Get investment project")
    register_resource("ancine", "/ancine", "List Ancine projects", "Get Ancine project")
    register_resource(
        "liberacoes-operacao-direta",
        "/liberacoes-operacao-direta",
        "List direct operation disbursements",
        "Get direct operation disbursement",
    )
    register_resource(
        "liberacoes-credito-descentralizado",
        "/liberacoes-credito-descentralizado",
        "List decentralized credit disbursements",
        "Get decentralized credit disbursement",
    )
    register_resource("liberacoes-ancine", "/liberacoes-ancine", "List Ancine disbursements", "Get Ancine disbursement")

    @router.get("/resumo-geral", response_model=FinepResumoGeralResponse, summary="Get FINEP general summary")
    async def get_resumo_geral(
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> FinepResumoGeralResponse:
        records = _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q)
        municipio_summary = await _build_municipio_summary(database, records)
        total_aprovado = Decimal("0.00")
        total_liberado = Decimal("0.00")
        empresas: set[str] = set()
        ufs: set[str] = set()
        fontes: set[str] = set()
        projetos_por_fonte: defaultdict[str, int] = defaultdict(int)
        for record in records:
            if record.get("cnpj_14"):
                empresas.add(record["cnpj_14"])
            if record.get("uf"):
                ufs.add(record["uf"])
            source = record["source"]
            fontes.add(source)
            projetos_por_fonte[source] += 1
            total_aprovado += record["total_aprovado_finep"]
            total_liberado += record["total_liberado"]
        municipios_por_uf: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in municipio_summary:
            if item["uf"]:
                municipios_por_uf[item["uf"]].append(item)
        return FinepResumoGeralResponse(
            total_empresas=len(empresas),
            total_municipios=len(municipio_summary),
            total_ufs=len(ufs),
            total_projetos=len(records),
            total_aprovado_finep=total_aprovado,
            total_liberado=total_liberado,
            ticket_medio_projeto=(total_aprovado / len(records) if records else Decimal("0.00")),
            gini_liberado=_calculate_gini([item["total_liberado"] for item in municipio_summary]),
            gini_por_uf=sorted(
                [
                    FinepGiniUfItem(
                        uf=uf_key,
                        total_municipios=len(items),
                        gini_liberado=_calculate_gini([municipality["total_liberado"] for municipality in items]),
                    )
                    for uf_key, items in municipios_por_uf.items()
                ],
                key=lambda item: item.gini_liberado,
                reverse=True,
            ),
            fontes=sorted(fontes),
            projetos_por_fonte=dict(sorted(projetos_por_fonte.items())),
        )

    @router.get("/resumo-empresa", response_model=PaginatedFinepResumoEmpresaResponse, summary="Get FINEP company summary")
    async def get_resumo_empresa(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedFinepResumoEmpresaResponse:
        groups: dict[str, dict[str, Any]] = {}
        for record in _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q):
            cnpj_14 = record.get("cnpj_14")
            if not cnpj_14:
                continue
            company = record.get("company") or {}
            if cnpj_14 not in groups:
                groups[cnpj_14] = {
                    "cnpj_14": cnpj_14,
                    "razao_social": company.get("razao_social"),
                    "nome_fantasia": company.get("nome_fantasia"),
                    "uf": record.get("uf") or company.get("uf"),
                    "municipio_nome": company.get("municipio_nome"),
                    "cnae_fiscal_principal": company.get("cnae_fiscal_principal"),
                    "cnae_descricao": company.get("cnae_descricao"),
                    "porte_empresa": company.get("porte_empresa"),
                    "natureza_juridica": company.get("natureza_juridica"),
                    "natureza_juridica_descricao": company.get("natureza_juridica_descricao"),
                    "capital_social": company.get("capital_social"),
                    "match_source": company.get("match_source"),
                    "total_projetos": 0,
                    "total_aprovado_finep": Decimal("0.00"),
                    "total_liberado": Decimal("0.00"),
                    "fontes": set(),
                }
            group = groups[cnpj_14]
            group["total_projetos"] += 1
            group["total_aprovado_finep"] += record["total_aprovado_finep"]
            group["total_liberado"] += record["total_liberado"]
            group["fontes"].add(record["source"])
        payload = [
            FinepResumoEmpresaItem.model_validate({**row, "fontes": sorted(row["fontes"])})
            for row in groups.values()
        ]
        payload.sort(key=lambda item: (item.total_aprovado_finep, item.total_projetos), reverse=True)
        page_items, total, next_url, previous_url = _paginate_items(request, payload, page, page_size)
        return PaginatedFinepResumoEmpresaResponse(count=total, next=next_url, previous=previous_url, results=page_items)

    @router.get("/resumo-uf", response_model=PaginatedFinepResumoUfResponse, summary="Get FINEP state summary")
    async def get_resumo_uf(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedFinepResumoUfResponse:
        groups: defaultdict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "total_empresas": set(),
                "total_projetos": 0,
                "total_aprovado_finep": Decimal("0.00"),
                "total_liberado": Decimal("0.00"),
                "fontes": set(),
            }
        )
        for record in _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q):
            key = record.get("uf") or "NA"
            group = groups[key]
            if record.get("cnpj_14"):
                group["total_empresas"].add(record["cnpj_14"])
            group["total_projetos"] += 1
            group["total_aprovado_finep"] += record["total_aprovado_finep"]
            group["total_liberado"] += record["total_liberado"]
            group["fontes"].add(record["source"])
        payload = [
            FinepResumoUfItem(
                uf=uf_key,
                total_empresas=len(row["total_empresas"]),
                total_projetos=row["total_projetos"],
                total_aprovado_finep=row["total_aprovado_finep"],
                total_liberado=row["total_liberado"],
                fontes=sorted(row["fontes"]),
            )
            for uf_key, row in groups.items()
        ]
        payload.sort(key=lambda item: (item.total_aprovado_finep, item.total_projetos), reverse=True)
        page_items, total, next_url, previous_url = _paginate_items(request, payload, page, page_size)
        return PaginatedFinepResumoUfResponse(count=total, next=next_url, previous=previous_url, results=page_items)

    @router.get("/resumo-cnae", response_model=PaginatedFinepResumoCnaeResponse, summary="Get FINEP CNAE summary")
    async def get_resumo_cnae(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedFinepResumoCnaeResponse:
        groups: defaultdict[tuple[str | None, str | None], dict[str, Any]] = defaultdict(
            lambda: {
                "total_empresas": set(),
                "total_projetos": 0,
                "total_aprovado_finep": Decimal("0.00"),
                "total_liberado": Decimal("0.00"),
                "fontes": set(),
            }
        )
        for record in _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q):
            company = record.get("company") or {}
            key = (company.get("cnae_fiscal_principal"), company.get("cnae_descricao"))
            group = groups[key]
            if record.get("cnpj_14"):
                group["total_empresas"].add(record["cnpj_14"])
            group["total_projetos"] += 1
            group["total_aprovado_finep"] += record["total_aprovado_finep"]
            group["total_liberado"] += record["total_liberado"]
            group["fontes"].add(record["source"])
        payload = [
            FinepResumoCnaeItem(
                cnae_fiscal_principal=key[0],
                cnae_descricao=key[1],
                total_empresas=len(row["total_empresas"]),
                total_projetos=row["total_projetos"],
                total_aprovado_finep=row["total_aprovado_finep"],
                total_liberado=row["total_liberado"],
                fontes=sorted(row["fontes"]),
            )
            for key, row in groups.items()
        ]
        payload.sort(key=lambda item: (item.total_aprovado_finep, item.total_projetos), reverse=True)
        page_items, total, next_url, previous_url = _paginate_items(request, payload, page, page_size)
        return PaginatedFinepResumoCnaeResponse(count=total, next=next_url, previous=previous_url, results=page_items)

    @router.get(
        "/resumo-municipio",
        response_model=PaginatedFinepResumoMunicipioResponse,
        summary="Get FINEP municipality summary",
    )
    async def get_resumo_municipio(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        all: bool = Query(default=False),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedFinepResumoMunicipioResponse:
        payload = [
            FinepResumoMunicipioItem.model_validate(item)
            for item in await _build_municipio_summary(
                database,
                _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q),
            )
        ]
        if all:
            return PaginatedFinepResumoMunicipioResponse(count=len(payload), next=None, previous=None, results=payload)
        page_items, total, next_url, previous_url = _paginate_items(request, payload, page, page_size)
        return PaginatedFinepResumoMunicipioResponse(count=total, next=next_url, previous=previous_url, results=page_items)

    @router.get("/mapa-municipio", response_model=FinepMunicipioMapaResponse, summary="Get FINEP municipality map")
    async def get_mapa_municipio(
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        simplify_tolerance: float = Query(default=0.0, ge=0.0),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> FinepMunicipioMapaResponse:
        records = _filter_analytics_records(await _collect_project_records(database), cnpj, uf, q)
        municipio_summary = await _build_municipio_summary(database, records)
        features_raw = await _load_geo_municipality_features(
            database,
            municipio_summary,
            simplify_tolerance=simplify_tolerance,
            uf=(uf or "").strip().upper() or None,
        )
        features = [FinepMapFeature.model_validate(feature) for feature in features_raw]
        max_approved_value = max((feature.properties["total_aprovado_finep"] for feature in features), default=0.0)
        municipalities_with_value = sum(1 for feature in features if feature.properties["total_aprovado_finep"] > 0)
        return FinepMunicipioMapaResponse(
            type="FeatureCollection",
            features=features,
            metadata=FinepMapMetadata(
                feature_count=len(features),
                municipalities_with_value=municipalities_with_value,
                max_total_aprovado_finep=max_approved_value,
                simplify_tolerance=simplify_tolerance,
            ),
        )

    return router
