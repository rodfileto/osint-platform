"""Pydantic response models for the unified search endpoint."""

from __future__ import annotations

from pydantic import BaseModel

from api.schemas.cnpj import CompanySearchItem


class UnifiedSearchPostgresResult(BaseModel):
    total: int
    results: list[CompanySearchItem]


class UnifiedSearchResponse(BaseModel):
    query: str
    normalized_query: str
    postgres: UnifiedSearchPostgresResult
