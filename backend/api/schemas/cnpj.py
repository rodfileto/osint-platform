"""Pydantic response models for the CNPJ API."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict


class CompanySearchItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    cnpj_14: str
    cnpj_basico: str
    razao_social: str
    nome_fantasia: str | None = None
    situacao_cadastral: str | None = None
    codigo_municipio: str | None = None
    municipio_nome: str | None = None
    uf: str | None = None
    cnae_fiscal_principal: str | None = None
    cnae_descricao: str | None = None
    porte_empresa: str | None = None
    natureza_juridica: int | None = None
    natureza_juridica_descricao: str | None = None
    capital_social: Decimal | None = None
    data_inicio_atividade: date | None = None
    correio_eletronico: str | None = None
    reference_month: str | None = None


class PaginatedCompanySearchResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[CompanySearchItem]


class PersonSearchItem(BaseModel):
    nome: str
    cpf_cnpj_socio: str
    faixa_etaria: int | None = None
    total_empresas: int


class PaginatedPersonSearchResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[PersonSearchItem]


class EstablishmentAddress(BaseModel):
    tipo_logradouro: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    complemento: str | None = None
    bairro: str | None = None
    cep: str | None = None
    uf: str | None = None
    municipio: str | None = None
    municipio_nome: str | None = None


class EstablishmentContact(BaseModel):
    ddd_telefone_1: str | None = None
    ddd_telefone_2: str | None = None
    ddd_fax: str | None = None
    email: str | None = None


class CompanyEstablishment(BaseModel):
    cnpj_completo: str
    cnpj_ordem: str
    cnpj_dv: str
    identificador_matriz_filial: int | None = None
    tipo_estabelecimento: str | None = None
    nome_fantasia: str | None = None
    situacao_cadastral: str | None = None
    situacao_cadastral_display: str | None = None
    data_situacao_cadastral: date | None = None
    motivo_situacao_cadastral: str | None = None
    motivo_situacao_cadastral_descricao: str | None = None
    data_inicio_atividade: date | None = None
    cnae_fiscal_principal: str | None = None
    cnae_fiscal_principal_descricao: str | None = None
    cnae_fiscal_secundaria: str | None = None
    pais: str | None = None
    pais_nome: str | None = None
    endereco: EstablishmentAddress
    contato: EstablishmentContact


class CompanyDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    cnpj_basico: str
    razao_social: str
    natureza_juridica: int | None = None
    natureza_juridica_descricao: str | None = None
    qualificacao_responsavel: str | None = None
    qualificacao_responsavel_descricao: str | None = None
    capital_social: Decimal | None = None
    porte_empresa: str | None = None
    porte_empresa_display: str | None = None
    ente_federativo_responsavel: str | None = None
    total_estabelecimentos: int
    estabelecimentos_ativos: int
    estabelecimentos: list[CompanyEstablishment]


class PersonCompanyItem(BaseModel):
    cnpj_basico: str
    razao_social: str
    qualificacao_socio: str | None = None
    qualificacao_socio_descricao: str | None = None
    data_entrada_sociedade: date | None = None
    situacao_cadastral: str | None = None
    uf: str | None = None
    municipio_nome: str | None = None
    reference_month: str


class PersonDetailResponse(BaseModel):
    cpf_mascarado: str
    nome: str | None = None
    faixa_etaria: int | None = None
    total_empresas: int
    empresas: list[PersonCompanyItem]


class GraphNode(BaseModel):
    id: str
    type: str
    label: str
    data: dict[str, Any]


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    type: str
    data: dict[str, Any]


class CompanyNetworkMetadata(BaseModel):
    core_cnpj_basico: str
    depth: int
    total_nodes: int
    total_edges: int
    total_relationships: int
    truncated: bool
    max_edges: int


class CompanyNetworkResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    metadata: CompanyNetworkMetadata


class PersonNetworkMetadata(BaseModel):
    core_cpf_mascarado: str
    core_nome: str | None = None
    depth: int
    total_nodes: int
    total_edges: int
    total_relationships: int
    truncated: bool
    max_edges: int


class PersonNetworkResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    metadata: PersonNetworkMetadata


class CnpjBootstrapStatus(BaseModel):
    domain: str
    status: str
    implemented_endpoints: list[str]
    pending_endpoints: list[str]
