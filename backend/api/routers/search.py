"""Unified search router over the PostgreSQL company search views."""

from __future__ import annotations

import unicodedata
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas.cnpj import CompanySearchItem
from api.schemas.search import (
    UnifiedSearchPostgresResult,
    UnifiedSearchResponse,
)
from core.database import DatabaseManager, get_database_manager

SEARCH_LIMIT_DEFAULT = 10
SEARCH_LIMIT_MAX = 50


def _digits_only(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _strip_accents(text: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFD", text.upper())
        if unicodedata.category(character) != "Mn"
    )


async def _search_postgres_companies(
    database: DatabaseManager,
    query: str,
    limit: int,
) -> UnifiedSearchPostgresResult:
    digits = _digits_only(query)
    normalized = _strip_accents(query)

    clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit}

    if digits:
        if len(digits) >= 14:
            clauses.append("(cnpj_14 = :cnpj_14 OR cnpj_basico = :cnpj_basico)")
            params["cnpj_14"] = digits[:14]
            params["cnpj_basico"] = digits[:8]
        elif len(digits) >= 8:
            clauses.append("(cnpj_basico = :cnpj_basico OR cnpj_14 LIKE :cnpj_prefix)")
            params["cnpj_basico"] = digits[:8]
            params["cnpj_prefix"] = f"{digits}%"
        else:
            clauses.append("(cnpj_basico LIKE :cnpj_basico_prefix OR cnpj_14 LIKE :cnpj_prefix)")
            params["cnpj_basico_prefix"] = f"{digits}%"
            params["cnpj_prefix"] = f"{digits}%"

    if normalized:
        clauses.append("(razao_social ILIKE :text_query OR nome_fantasia ILIKE :text_query)")
        params["text_query"] = f"%{normalized}%"

    where_clause = f"WHERE {' OR '.join(clauses)}" if clauses else ""
    rows = await database.fetch_all(
        f"""
        WITH ranked_matches AS (
            SELECT
                cnpj_14,
                cnpj_basico,
                razao_social,
                nome_fantasia,
                situacao_cadastral,
                codigo_municipio,
                municipio_nome,
                uf,
                cnae_fiscal_principal,
                cnae_descricao,
                porte_empresa,
                natureza_juridica,
                natureza_juridica_descricao,
                capital_social,
                data_inicio_atividade,
                correio_eletronico,
                reference_month,
                'active' AS match_source
            FROM cnpj.mv_company_search
            {where_clause}

            UNION ALL

            SELECT
                cnpj_14,
                cnpj_basico,
                razao_social,
                nome_fantasia,
                situacao_cadastral,
                codigo_municipio,
                municipio_nome,
                uf,
                cnae_fiscal_principal,
                cnae_descricao,
                porte_empresa,
                natureza_juridica,
                natureza_juridica_descricao,
                capital_social,
                data_inicio_atividade,
                correio_eletronico,
                reference_month,
                'inactive' AS match_source
            FROM cnpj.mv_company_search_inactive
            {where_clause}
        )
        SELECT DISTINCT ON (cnpj_14)
            cnpj_14,
            cnpj_basico,
            razao_social,
            nome_fantasia,
            situacao_cadastral,
            codigo_municipio,
            municipio_nome,
            uf,
            cnae_fiscal_principal,
            cnae_descricao,
            porte_empresa,
            natureza_juridica,
            natureza_juridica_descricao,
            capital_social,
            data_inicio_atividade,
            correio_eletronico,
            reference_month
        FROM ranked_matches
        ORDER BY cnpj_14, match_source, razao_social
        LIMIT :limit
        """,
        params,
    )
    results = [CompanySearchItem.model_validate(row) for row in rows]
    return UnifiedSearchPostgresResult(total=len(results), results=results)


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("", response_model=UnifiedSearchResponse, summary="Unified search")
    async def unified_search(
        q: str = Query(..., min_length=1),
        limit: int = Query(default=SEARCH_LIMIT_DEFAULT, ge=1, le=SEARCH_LIMIT_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> UnifiedSearchResponse:
        query = q.strip()
        if not query:
            raise HTTPException(status_code=422, detail="Informe um termo de busca.")

        return UnifiedSearchResponse(
            query=query,
            normalized_query=_strip_accents(query),
            postgres=await _search_postgres_companies(database, query, limit),
        )

    return router