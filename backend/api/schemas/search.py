"""Pydantic response models for the unified search endpoint."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from api.schemas.cnpj import CompanySearchItem


class UnifiedSearchNeo4jMatch(BaseModel):
    type: str
    identifier: str
    label: str
    relationship_count: int
    data: dict[str, Any]


class UnifiedSearchPostgresResult(BaseModel):
    total: int
    results: list[CompanySearchItem]


class UnifiedSearchNeo4jResult(BaseModel):
    total: int
    results: list[UnifiedSearchNeo4jMatch]


class UnifiedSearchResponse(BaseModel):
    query: str
    normalized_query: str
    postgres: UnifiedSearchPostgresResult
    neo4j: UnifiedSearchNeo4jResult
