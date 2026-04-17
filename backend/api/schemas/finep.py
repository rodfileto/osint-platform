"""Pydantic response models for the FINEP API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict


class FinepCompanyLookup(BaseModel):
    model_config = ConfigDict(extra="allow")

    cnpj_14: str
    razao_social: str | None = None
    nome_fantasia: str | None = None
    situacao_cadastral: str | None = None
    municipio_nome: str | None = None
    uf: str | None = None
    cnae_fiscal_principal: str | None = None
    cnae_descricao: str | None = None
    porte_empresa: str | None = None
    natureza_juridica: int | None = None
    natureza_juridica_descricao: str | None = None
    capital_social: Decimal | None = None
    data_inicio_atividade: date | None = None
    reference_month: str | None = None
    match_source: str | None = None


class FinepRelatedProject(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    contrato: str | None = None
    ref: str | None = None
    titulo: str | None = None
    proponente: str | None = None
    cnpj_proponente_norm: str | None = None
    executor: str | None = None
    status: str | None = None
    instrumento: str | None = None
    uf: str | None = None
    data_assinatura: date | None = None
    valor_finep: Decimal | None = None
    valor_pago: Decimal | None = None
    proponente_empresa: FinepCompanyLookup | None = None


class FinepResourceRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    created_at: datetime | None = None
    updated_at: datetime | None = None
    proponente_empresa: FinepCompanyLookup | None = None
    executor_empresa: FinepCompanyLookup | None = None
    projeto: FinepRelatedProject | None = None


class PaginatedFinepResourceResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FinepResourceRecord]


class FinepBootstrapStatus(BaseModel):
    domain: str
    status: str
    implemented_endpoints: list[str]
    pending_endpoints: list[str]


class FinepResumoEmpresaItem(BaseModel):
    cnpj_14: str
    razao_social: str | None = None
    nome_fantasia: str | None = None
    uf: str | None = None
    municipio_nome: str | None = None
    cnae_fiscal_principal: str | None = None
    cnae_descricao: str | None = None
    porte_empresa: str | None = None
    natureza_juridica: int | None = None
    natureza_juridica_descricao: str | None = None
    capital_social: Decimal | None = None
    match_source: str | None = None
    total_projetos: int
    total_aprovado_finep: Decimal
    total_liberado: Decimal
    fontes: list[str]


class FinepGiniUfItem(BaseModel):
    uf: str
    total_municipios: int
    gini_liberado: float


class FinepResumoGeralResponse(BaseModel):
    total_empresas: int
    total_municipios: int
    total_ufs: int
    total_projetos: int
    total_aprovado_finep: Decimal
    total_liberado: Decimal
    ticket_medio_projeto: Decimal
    gini_liberado: float
    gini_por_uf: list[FinepGiniUfItem]
    fontes: list[str]
    projetos_por_fonte: dict[str, int]


class FinepResumoUfItem(BaseModel):
    uf: str
    total_empresas: int
    total_projetos: int
    total_aprovado_finep: Decimal
    total_liberado: Decimal
    fontes: list[str]


class FinepResumoCnaeItem(BaseModel):
    cnae_fiscal_principal: str | None = None
    cnae_descricao: str | None = None
    total_empresas: int
    total_projetos: int
    total_aprovado_finep: Decimal
    total_liberado: Decimal
    fontes: list[str]


class FinepResumoMunicipioItem(BaseModel):
    municipio_nome: str
    uf: str | None = None
    municipality_ibge_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    total_empresas: int
    total_projetos: int
    total_aprovado_finep: Decimal
    total_liberado: Decimal
    fontes: list[str]


class PaginatedFinepResumoEmpresaResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FinepResumoEmpresaItem]


class PaginatedFinepResumoUfResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FinepResumoUfItem]


class PaginatedFinepResumoCnaeResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FinepResumoCnaeItem]


class PaginatedFinepResumoMunicipioResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FinepResumoMunicipioItem]


class FinepMapFeature(BaseModel):
    type: str
    id: str
    geometry: dict[str, Any] | None = None
    properties: dict[str, Any]


class FinepMapMetadata(BaseModel):
    feature_count: int
    municipalities_with_value: int
    max_total_aprovado_finep: float
    simplify_tolerance: float


class FinepMunicipioMapaResponse(BaseModel):
    type: str
    features: list[FinepMapFeature]
    metadata: FinepMapMetadata
