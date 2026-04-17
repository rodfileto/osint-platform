"""Unified search router combining Postgres and Neo4j results."""

from __future__ import annotations

import asyncio
import unicodedata
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas.cnpj import CompanySearchItem
from api.schemas.search import (
    UnifiedSearchNeo4jMatch,
    UnifiedSearchNeo4jResult,
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
        clauses.append("(razao_social ILIKE :text_query OR COALESCE(nome_fantasia, '') ILIKE :text_query)")
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


async def _search_neo4j_preview(
    database: DatabaseManager,
    query: str,
    limit: int,
) -> UnifiedSearchNeo4jResult:
    digits = _digits_only(query)
    normalized = _strip_accents(query)
    neo4j_query = """
    MATCH (n)
    WHERE (
        n:Empresa AND (
            ($digits <> '' AND coalesce(n.cnpj_basico, '') STARTS WITH $digits)
            OR ($normalized <> '' AND toUpper(coalesce(n.razao_social, coalesce(n.nome, ''))) CONTAINS $normalized)
        )
    ) OR (
        n:Pessoa AND $normalized <> '' AND toUpper(coalesce(n.nome, '')) CONTAINS $normalized
    )
    WITH DISTINCT n,
         CASE
             WHEN $digits <> '' AND coalesce(n.cnpj_basico, '') = $digits THEN 0
             WHEN $digits <> '' AND coalesce(n.cnpj_basico, '') STARTS WITH $digits THEN 1
             ELSE 2
         END AS rank
    ORDER BY rank, coalesce(n.razao_social, n.nome, n.cnpj_basico, n.cpf_cnpj_socio)
    LIMIT $limit
    OPTIONAL MATCH (n)-[r:SOCIO_DE]-()
    RETURN collect({
        type: CASE WHEN n:Empresa THEN 'empresa' ELSE 'pessoa' END,
        identifier: coalesce(n.cnpj_basico, n.cpf_cnpj_socio, elementId(n)),
        label: coalesce(n.razao_social, n.nome, n.cnpj_basico, n.cpf_cnpj_socio, 'Desconhecido'),
        relationship_count: count(r),
        data: {
            cnpj_basico: n.cnpj_basico,
            cpf_cnpj_socio: n.cpf_cnpj_socio,
            porte_empresa: n.porte_empresa,
            natureza_juridica: n.natureza_juridica,
            faixa_etaria: n.faixa_etaria,
            identificador_socio: n.identificador_socio
        }
    }) AS results
    """
    try:
        rows = await database.run_neo4j_query(
            neo4j_query,
            {
                "digits": digits[:8],
                "normalized": normalized,
                "limit": limit,
            },
        )
    except Exception as exc:  # pragma: no cover - driver/network errors are environment-specific
        raise HTTPException(status_code=503, detail="Falha ao consultar Neo4j no endpoint unificado de busca.") from exc

    raw_results = rows[0]["results"] if rows else []
    results = [UnifiedSearchNeo4jMatch.model_validate(item) for item in raw_results]
    return UnifiedSearchNeo4jResult(total=len(results), results=results)


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("", response_model=UnifiedSearchResponse, summary="Unified concurrent search")
    async def unified_search(
        q: str = Query(..., min_length=1),
        limit: int = Query(default=SEARCH_LIMIT_DEFAULT, ge=1, le=SEARCH_LIMIT_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> UnifiedSearchResponse:
        query = q.strip()
        if not query:
            raise HTTPException(status_code=422, detail="Informe um termo de busca.")

        postgres_result, neo4j_result = await asyncio.gather(
            _search_postgres_companies(database, query, limit),
            _search_neo4j_preview(database, query, limit),
        )
        return UnifiedSearchResponse(
            query=query,
            normalized_query=_strip_accents(query),
            postgres=postgres_result,
            neo4j=neo4j_result,
        )

    return router