"""Pydantic response models for the Inovalink API."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class InovalinkOrganizationRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    manifest_id: int
    source_collection: str
    source_record_id: int
    entity_kind: str
    category_id: int | None = None
    category_label: str | None = None
    source_linkable_type: str | None = None
    source_linkable_id: int | None = None
    name: str
    acronym: str | None = None
    corporate_name: str | None = None
    cnpj: str | None = None
    telephone: str | None = None
    email: str | None = None
    site: str | None = None
    instagram: str | None = None
    linkedin: str | None = None
    logo_path: str | None = None
    institution: str | None = None
    active: bool | None = None
    city_ibge_code: str | None = None
    city_name: str | None = None
    state_ibge_code: str | None = None
    state_name: str | None = None
    state_abbrev: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    source_created_at: datetime | None = None
    source_updated_at: datetime | None = None
    collected_at: datetime | None = None
    snapshot_date: date | None = None
    manifest_processing_status: str | None = None


class PaginatedInovalinkOrganizationResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[InovalinkOrganizationRecord]


class InovalinkMunicipalityValidationRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_collection: str
    source_record_id: int
    entity_kind: str
    category_label: str | None = None
    name: str
    city_name: str | None = None
    state_name: str | None = None
    state_abbrev: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geobr_municipality_ibge_code: str | None = None
    geobr_city_name: str | None = None
    geobr_state_abbrev: str | None = None
    validation_status: str
    municipality_matches: bool | None = None


class PaginatedInovalinkMunicipalityValidationResponse(BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[InovalinkMunicipalityValidationRecord]


class InovalinkMunicipalityCityCount(BaseModel):
    city_name: str
    state_abbrev: str | None = None
    total_records: int


class InovalinkMunicipalityValidationSummaryResponse(BaseModel):
    total_records: int
    issue_records: int
    top_declared_cities: list[InovalinkMunicipalityCityCount]
    top_geobr_cities: list[InovalinkMunicipalityCityCount]


class InovalinkBootstrapStatus(BaseModel):
    domain: str
    status: str
    implemented_endpoints: list[str]
    pending_endpoints: list[str]