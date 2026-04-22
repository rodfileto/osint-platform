"""Inovalink FastAPI router backed by the latest Postgres snapshot view."""

from __future__ import annotations

import unicodedata
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.schemas.inovalink import (
    InovalinkBootstrapStatus,
    InovalinkMunicipalityCityCount,
    InovalinkMunicipalityValidationRecord,
    InovalinkMunicipalityValidationSummaryResponse,
    InovalinkOrganizationRecord,
    PaginatedInovalinkMunicipalityValidationResponse,
    PaginatedInovalinkOrganizationResponse,
)
from core.database import DatabaseManager, get_database_manager

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 100

IMPLEMENTED_ENDPOINTS = [
    "/organizations",
    "/organizations/{source_collection}/{source_record_id}",
    "/municipality-validation",
    "/municipality-validation/summary",
]

PENDING_ENDPOINTS: list[str] = []


def _build_page_url(request: Request, page: int, page_size: int) -> str:
    return str(request.url.include_query_params(page=page, page_size=page_size))


def _normalize_record(row: dict[str, Any]) -> InovalinkOrganizationRecord:
    return InovalinkOrganizationRecord.model_validate(row)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.strip().split())
    if not collapsed:
        return None
    normalized = unicodedata.normalize("NFD", collapsed.casefold())
    return "".join(character for character in normalized if unicodedata.category(character) != "Mn")


def _validation_status(row: dict[str, Any]) -> tuple[str, bool | None]:
    latitude = row.get("latitude")
    longitude = row.get("longitude")
    geobr_city_name = row.get("geobr_city_name")
    geobr_state_abbrev = row.get("geobr_state_abbrev")

    if latitude is None or longitude is None:
        return "coordinates_missing", None
    if geobr_city_name is None or geobr_state_abbrev is None:
        return "polygon_not_found", None
    if (row.get("state_abbrev") or "").strip().upper() != geobr_state_abbrev.strip().upper():
        return "state_mismatch", False

    matches = _normalize_text(row.get("city_name")) == _normalize_text(geobr_city_name)
    if matches:
        return "ok", True
    return "municipality_mismatch", False


def _normalize_validation_record(row: dict[str, Any]) -> InovalinkMunicipalityValidationRecord:
    status, municipality_matches = _validation_status(row)
    payload = dict(row)
    payload["validation_status"] = status
    payload["municipality_matches"] = municipality_matches
    return InovalinkMunicipalityValidationRecord.model_validate(payload)


def _build_filters(
    q: str | None,
    entity_kind: str | None,
    category_label: str | None,
    city_name: str | None,
    state_name: str | None,
    state_abbrev: str | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}

    if q:
        clauses.append(
            """
            (
                name ILIKE :q
                OR COALESCE(corporate_name, '') ILIKE :q
                OR COALESCE(acronym, '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{q.strip()}%"

    if entity_kind:
        clauses.append("entity_kind = :entity_kind")
        params["entity_kind"] = entity_kind.strip().lower()

    if category_label:
        clauses.append("category_label ILIKE :category_label")
        params["category_label"] = f"%{category_label.strip()}%"

    if city_name:
        clauses.append("city_name ILIKE :city_name")
        params["city_name"] = f"%{city_name.strip()}%"

    if state_name:
        clauses.append("state_name ILIKE :state_name")
        params["state_name"] = f"%{state_name.strip()}%"

    if state_abbrev:
        clauses.append("state_abbrev = :state_abbrev")
        params["state_abbrev"] = state_abbrev.strip().upper()

    if not clauses:
        return "", params

    return f"WHERE {' AND '.join(clauses)}", params


def _build_validation_filters(
    q: str | None,
    entity_kind: str | None,
    state_abbrev: str | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}

    if q:
        clauses.append(
            """
            (
                name ILIKE :q
                OR COALESCE(city_name, '') ILIKE :q
                OR COALESCE(category_label, '') ILIKE :q
                OR COALESCE(source_collection, '') ILIKE :q
            )
            """
        )
        params["q"] = f"%{q.strip()}%"

    if entity_kind:
        clauses.append("entity_kind = :entity_kind")
        params["entity_kind"] = entity_kind.strip().lower()

    if state_abbrev:
        clauses.append("state_abbrev = :state_abbrev")
        params["state_abbrev"] = state_abbrev.strip().upper()

    if not clauses:
        return "", params

    return f"WHERE {' AND '.join(clauses)}", params


async def _list_organizations(
    database: DatabaseManager,
    request: Request,
    q: str | None,
    entity_kind: str | None,
    category_label: str | None,
    city_name: str | None,
    state_name: str | None,
    state_abbrev: str | None,
    page: int,
    page_size: int,
) -> PaginatedInovalinkOrganizationResponse:
    where_clause, params = _build_filters(
        q=q,
        entity_kind=entity_kind,
        category_label=category_label,
        city_name=city_name,
        state_name=state_name,
        state_abbrev=state_abbrev,
    )
    offset = (page - 1) * page_size

    total_row = await database.fetch_one(
        f"SELECT COUNT(*) AS total FROM inovalink.latest_organization_snapshot {where_clause}",
        params,
    )
    total = int((total_row or {}).get("total", 0))

    rows = await database.fetch_all(
        f"""
        SELECT
            id,
            manifest_id,
            source_collection,
            source_record_id,
            entity_kind,
            category_id,
            category_label,
            source_linkable_type,
            source_linkable_id,
            name,
            acronym,
            corporate_name,
            cnpj,
            telephone,
            email,
            site,
            instagram,
            linkedin,
            logo_path,
            institution,
            active,
            city_ibge_code,
            city_name,
            state_ibge_code,
            state_name,
            state_abbrev,
            latitude,
            longitude,
            source_created_at,
            source_updated_at,
            collected_at,
            snapshot_date,
            manifest_processing_status
        FROM inovalink.latest_organization_snapshot
        {where_clause}
        ORDER BY entity_kind ASC, category_label ASC NULLS LAST, name ASC
        LIMIT :limit OFFSET :offset
        """,
        {**params, "limit": page_size, "offset": offset},
    )

    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return PaginatedInovalinkOrganizationResponse(
        count=total,
        next=next_url,
        previous=previous_url,
        results=[_normalize_record(row) for row in rows],
    )


async def _get_organization(
    database: DatabaseManager,
    source_collection: str,
    source_record_id: int,
) -> InovalinkOrganizationRecord:
    row = await database.fetch_one(
        """
        SELECT
            id,
            manifest_id,
            source_collection,
            source_record_id,
            entity_kind,
            category_id,
            category_label,
            source_linkable_type,
            source_linkable_id,
            name,
            acronym,
            corporate_name,
            cnpj,
            telephone,
            email,
            site,
            instagram,
            linkedin,
            logo_path,
            institution,
            active,
            city_ibge_code,
            city_name,
            state_ibge_code,
            state_name,
            state_abbrev,
            latitude,
            longitude,
            source_created_at,
            source_updated_at,
            collected_at,
            snapshot_date,
            manifest_processing_status
        FROM inovalink.latest_organization_snapshot
        WHERE source_collection = :source_collection
          AND source_record_id = :source_record_id
        LIMIT 1
        """,
        {
            "source_collection": source_collection,
            "source_record_id": source_record_id,
        },
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Registro Inovalink não encontrado.")
    return _normalize_record(row)


def _build_validation_issue_clause(only_issues: bool) -> str:
    conditions = _build_validation_issue_conditions(only_issues)
    if not conditions:
        return ""
    return f"WHERE ({' OR '.join(conditions)})"


def _build_validation_issue_conditions(only_issues: bool) -> list[str]:
    if not only_issues:
        return []
    return [
        "latitude IS NULL",
        "longitude IS NULL",
        "geobr_municipality_ibge_code IS NULL",
        "UPPER(COALESCE(state_abbrev, '')) <> UPPER(COALESCE(geobr_state_abbrev, ''))",
        "LOWER(BTRIM(COALESCE(city_name, ''))) <> LOWER(BTRIM(COALESCE(geobr_city_name, '')))",
    ]


def _build_non_empty_city_clause(city_column: str) -> str:
    return f"COALESCE(BTRIM({city_column}), '') <> ''"


def _combine_conditions(conditions: list[str], operator: str) -> str:
    if not conditions:
        return ""
    return f"WHERE ({f' {operator} '.join(conditions)})"


def _build_validation_cte(where_clause: str) -> str:
    return f"""
    WITH scoped AS (
        SELECT
            id,
            source_collection,
            source_record_id,
            entity_kind,
            category_label,
            name,
            city_name,
            state_name,
            state_abbrev,
            latitude,
            longitude,
            CASE
                WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN public.ST_Transform(
                    public.ST_SetSRID(public.ST_MakePoint(longitude, latitude), 4326),
                    4674
                )
                ELSE NULL
            END AS point_geom
        FROM inovalink.latest_organization_snapshot
        {where_clause}
    ),
    enriched AS (
        SELECT
            scoped.*,
            geobr_match.municipality_ibge_code AS geobr_municipality_ibge_code,
            geobr_match.municipality_name AS geobr_city_name,
            geobr_match.state_abbrev AS geobr_state_abbrev
        FROM scoped
        LEFT JOIN LATERAL (
            SELECT
                municipality_ibge_code,
                municipality_name,
                state_abbrev
            FROM geo.br_municipality
            WHERE scoped.point_geom IS NOT NULL
              AND geom && scoped.point_geom
              AND public.ST_Covers(geom, scoped.point_geom)
            ORDER BY CASE WHEN geo.br_municipality.state_abbrev = scoped.state_abbrev THEN 0 ELSE 1 END
            LIMIT 1
        ) AS geobr_match ON TRUE
    )
    """


async def _list_municipality_validation(
    database: DatabaseManager,
    request: Request,
    q: str | None,
    entity_kind: str | None,
    state_abbrev: str | None,
    only_issues: bool,
    page: int,
    page_size: int,
) -> PaginatedInovalinkMunicipalityValidationResponse:
    where_clause, params = _build_validation_filters(q=q, entity_kind=entity_kind, state_abbrev=state_abbrev)
    issue_clause = _build_validation_issue_clause(only_issues)
    validation_cte = _build_validation_cte(where_clause)
    offset = (page - 1) * page_size

    rows = await database.fetch_all(
        f"""
        {validation_cte}
        SELECT
            COUNT(*) OVER() AS total_count,
            id,
            source_collection,
            source_record_id,
            entity_kind,
            category_label,
            name,
            city_name,
            state_name,
            state_abbrev,
            latitude,
            longitude,
            geobr_municipality_ibge_code,
            geobr_city_name,
            geobr_state_abbrev
        FROM enriched
        {issue_clause}
        ORDER BY
            CASE
                WHEN latitude IS NULL OR longitude IS NULL THEN 0
                WHEN geobr_municipality_ibge_code IS NULL THEN 1
                WHEN UPPER(COALESCE(state_abbrev, '')) <> UPPER(COALESCE(geobr_state_abbrev, '')) THEN 2
                WHEN LOWER(BTRIM(COALESCE(city_name, ''))) <> LOWER(BTRIM(COALESCE(geobr_city_name, ''))) THEN 3
                ELSE 4
            END,
            name ASC,
            source_record_id ASC
        LIMIT :limit OFFSET :offset
        """,
        {**params, "limit": page_size, "offset": offset},
    )

    total = int(rows[0]["total_count"]) if rows else 0
    normalized_rows = []
    for row in rows:
        payload = dict(row)
        payload.pop("total_count", None)
        normalized_rows.append(_normalize_validation_record(payload))

    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return PaginatedInovalinkMunicipalityValidationResponse(
        count=total,
        next=next_url,
        previous=previous_url,
        results=normalized_rows,
    )


async def _get_municipality_validation_summary(
    database: DatabaseManager,
    q: str | None,
    entity_kind: str | None,
    state_abbrev: str | None,
    only_issues: bool,
) -> InovalinkMunicipalityValidationSummaryResponse:
    where_clause, params = _build_validation_filters(q=q, entity_kind=entity_kind, state_abbrev=state_abbrev)
    issue_clause = _build_validation_issue_clause(only_issues)
    validation_cte = _build_validation_cte(where_clause)

    total_row = await database.fetch_one(
        f"""
        {validation_cte}
        SELECT COUNT(*) AS total_records
        FROM enriched
        """,
        params,
    )
    total_records = int((total_row or {}).get("total_records", 0))

    issue_row = await database.fetch_one(
        f"""
        {validation_cte}
        SELECT COUNT(*) AS issue_records
        FROM enriched
        {_build_validation_issue_clause(True)}
        """,
        params,
    )
    issue_records = int((issue_row or {}).get("issue_records", 0))

    declared_where_parts = _build_validation_issue_conditions(only_issues)
    declared_where_parts.append(_build_non_empty_city_clause("city_name"))
    declared_where_clause = (
        f"WHERE ({' OR '.join(_build_validation_issue_conditions(only_issues))}) AND {_build_non_empty_city_clause('city_name')}"
        if only_issues
        else _combine_conditions(declared_where_parts, "AND")
    )

    geobr_where_parts = _build_validation_issue_conditions(only_issues)
    geobr_where_parts.append(_build_non_empty_city_clause("geobr_city_name"))
    geobr_where_clause = (
        f"WHERE ({' OR '.join(_build_validation_issue_conditions(only_issues))}) AND {_build_non_empty_city_clause('geobr_city_name')}"
        if only_issues
        else _combine_conditions(geobr_where_parts, "AND")
    )

    declared_rows = await database.fetch_all(
        f"""
        {validation_cte}
        SELECT
            city_name,
            state_abbrev,
            COUNT(*) AS total_records
        FROM enriched
        {declared_where_clause}
        GROUP BY city_name, state_abbrev
        ORDER BY total_records DESC, city_name ASC, state_abbrev ASC NULLS LAST
        LIMIT 10
        """,
        params,
    )

    geobr_rows = await database.fetch_all(
        f"""
        {validation_cte}
        SELECT
            geobr_city_name AS city_name,
            geobr_state_abbrev AS state_abbrev,
            COUNT(*) AS total_records
        FROM enriched
        {geobr_where_clause}
        GROUP BY geobr_city_name, geobr_state_abbrev
        ORDER BY total_records DESC, geobr_city_name ASC, geobr_state_abbrev ASC NULLS LAST
        LIMIT 10
        """,
        params,
    )

    return InovalinkMunicipalityValidationSummaryResponse(
        total_records=total_records,
        issue_records=issue_records,
        top_declared_cities=[InovalinkMunicipalityCityCount.model_validate(row) for row in declared_rows],
        top_geobr_cities=[InovalinkMunicipalityCityCount.model_validate(row) for row in geobr_rows],
    )


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", response_model=InovalinkBootstrapStatus, summary="Inovalink migration status")
    async def inovalink_bootstrap_status() -> InovalinkBootstrapStatus:
        return InovalinkBootstrapStatus(
            domain="inovalink",
            status="implemented",
            implemented_endpoints=IMPLEMENTED_ENDPOINTS,
            pending_endpoints=PENDING_ENDPOINTS,
        )

    @router.get(
        "/organizations",
        response_model=PaginatedInovalinkOrganizationResponse,
        summary="List Inovalink organizations",
    )
    async def list_organizations(
        request: Request,
        q: str | None = Query(default=None),
        entity_kind: str | None = Query(default=None),
        category_label: str | None = Query(default=None),
        city_name: str | None = Query(default=None),
        state_name: str | None = Query(default=None),
        state_abbrev: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInovalinkOrganizationResponse:
        return await _list_organizations(
            database=database,
            request=request,
            q=q,
            entity_kind=entity_kind,
            category_label=category_label,
            city_name=city_name,
            state_name=state_name,
            state_abbrev=state_abbrev,
            page=page,
            page_size=page_size,
        )

    @router.get(
        "/organizations/{source_collection}/{source_record_id}",
        response_model=InovalinkOrganizationRecord,
        summary="Get Inovalink organization by source key",
    )
    async def get_organization(
        source_collection: str,
        source_record_id: int,
        database: DatabaseManager = Depends(get_database_manager),
    ) -> InovalinkOrganizationRecord:
        return await _get_organization(database, source_collection, source_record_id)

    @router.get(
        "/municipality-validation",
        response_model=PaginatedInovalinkMunicipalityValidationResponse,
        summary="Validate Inovalink municipalities with GeoBR polygons",
    )
    async def list_municipality_validation(
        request: Request,
        q: str | None = Query(default=None),
        entity_kind: str | None = Query(default=None),
        state_abbrev: str | None = Query(default=None),
        only_issues: bool = Query(default=False),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInovalinkMunicipalityValidationResponse:
        return await _list_municipality_validation(
            database=database,
            request=request,
            q=q,
            entity_kind=entity_kind,
            state_abbrev=state_abbrev,
            only_issues=only_issues,
            page=page,
            page_size=page_size,
        )

    @router.get(
        "/municipality-validation/summary",
        response_model=InovalinkMunicipalityValidationSummaryResponse,
        summary="Summarize Inovalink municipality validation with top declared and GeoBR cities",
    )
    async def municipality_validation_summary(
        q: str | None = Query(default=None),
        entity_kind: str | None = Query(default=None),
        state_abbrev: str | None = Query(default=None),
        only_issues: bool = Query(default=False),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> InovalinkMunicipalityValidationSummaryResponse:
        return await _get_municipality_validation_summary(
            database=database,
            q=q,
            entity_kind=entity_kind,
            state_abbrev=state_abbrev,
            only_issues=only_issues,
        )

    return router