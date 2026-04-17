"""Pydantic response models for the INPI API."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class InpiCompanyLookup(BaseModel):
    model_config = ConfigDict(extra="allow")

    cnpj_14: str
    cnpj_basico: str
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


class InpiRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    empresa: InpiCompanyLookup | None = None
    depositante_empresa: InpiCompanyLookup | None = None


class PaginatedInpiRecordResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[InpiRecord]


class InpiResumoTipoItem(BaseModel):
    tipo_patente: str | None = None
    total: int


class InpiResumoIPCItem(BaseModel):
    ipc_secao_classe: str | None = None
    total: int
    titulo_en: str | None = None


class InpiResumoAnoItem(BaseModel):
    ano: int | None = None
    total: int


class InpiResumoResponse(BaseModel):
    total_patentes: int
    por_tipo: list[InpiResumoTipoItem]
    por_secao_ipc: list[InpiResumoIPCItem]
    por_ano_deposito: list[InpiResumoAnoItem]
    snapshot_date: str | None = None


class InpiBootstrapStatus(BaseModel):
    domain: str
    status: str
    implemented_endpoints: list[str]
    pending_endpoints: list[str]
