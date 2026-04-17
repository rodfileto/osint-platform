"""CNPJ FastAPI router backed by PostgreSQL materialized views and tables."""

from __future__ import annotations

import unicodedata
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.schemas.cnpj import (
    CnpjBootstrapStatus,
    CompanyDetailResponse,
    CompanyEstablishment,
    CompanyNetworkResponse,
    CompanySearchItem,
    EstablishmentAddress,
    EstablishmentContact,
    PaginatedCompanySearchResponse,
    PaginatedPersonSearchResponse,
    PersonCompanyItem,
    PersonDetailResponse,
    PersonNetworkResponse,
    PersonSearchItem,
)
from api.services.cnpj_network import CnpjNetworkService
from core.database import DatabaseManager, get_database_manager

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 100

PORTE_EMPRESA_LABELS = {
    "00": "Não informado",
    "01": "Micro empresa",
    "03": "Empresa de pequeno porte",
    "05": "Demais",
}

MATRIZ_FILIAL_LABELS = {
    1: "Matriz",
    2: "Filial",
}

SITUACAO_CADASTRAL_LABELS = {
    "01": "Nula",
    "02": "Ativa",
    "03": "Suspensa",
    "04": "Inapta",
    "08": "Baixada",
}

IMPLEMENTED_ENDPOINTS = [
    "/search",
    "/search/{cnpj_14}",
    "/search-inactive",
    "/search-inactive/{cnpj_14}",
    "/empresa/{cnpj_basico}",
    "/pessoa/search",
    "/pessoa/detail",
    "/pessoa/{cpf}",
]

PENDING_ENDPOINTS = [
]


def _strip_accents(text: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFD", text.upper())
        if unicodedata.category(character) != "Mn"
    )


def _digits_only(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _format_phone(ddd: str | None, number: Any) -> str | None:
    if not ddd or number is None:
        return None

    digits = str(number)
    if len(digits) == 9:
        return f"({ddd}) {digits[:5]}-{digits[5:]}"
    if len(digits) == 8:
        return f"({ddd}) {digits[:4]}-{digits[4:]}"
    return f"({ddd}) {digits}"


def _mask_cpf(cpf_raw: str) -> str | None:
    digits = _digits_only(cpf_raw)
    if len(digits) != 11:
        return None
    return f"***{digits[3:9]}**"


def _normalize_masked_cpf(cpf_masked: str) -> str | None:
    normalized = cpf_masked.strip().replace(" ", "")
    if len(normalized) != 11:
        return None
    if normalized[:3] != "***" or normalized[-2:] != "**":
        return None
    middle = normalized[3:9]
    if not middle.isdigit():
        return None
    return normalized


def _build_page_url(request: Request, page: int, page_size: int) -> str:
    return str(request.url.include_query_params(page=page, page_size=page_size))


def _normalize_company_search_row(row: dict[str, Any]) -> CompanySearchItem:
    return CompanySearchItem.model_validate(row)


def _normalize_person_search_row(row: dict[str, Any]) -> PersonSearchItem:
    return PersonSearchItem.model_validate(row)


def _build_company_search_filters(
    q: str | None,
    uf: str | None,
    municipio: str | None,
    cnae: str | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    parameters: dict[str, Any] = {}

    if q:
        digits_only = _digits_only(q)
        if digits_only.isdigit() and digits_only:
            if len(digits_only) == 14:
                clauses.append("cnpj_14 = :cnpj_14")
                parameters["cnpj_14"] = digits_only
            elif len(digits_only) >= 9:
                clauses.append("cnpj_14 LIKE :cnpj_14_prefix")
                parameters["cnpj_14_prefix"] = f"{digits_only}%"
            else:
                clauses.append("cnpj_basico LIKE :cnpj_basico_prefix")
                parameters["cnpj_basico_prefix"] = f"{digits_only[:8]}%"
        else:
            normalized_query = _strip_accents(q)
            clauses.append(
                "(razao_social ILIKE :text_query OR COALESCE(nome_fantasia, '') ILIKE :text_query)"
            )
            parameters["text_query"] = f"%{normalized_query}%"

    if uf:
        clauses.append("uf = :uf")
        parameters["uf"] = uf.strip().upper()

    if municipio:
        clauses.append("municipio_nome ILIKE :municipio")
        parameters["municipio"] = f"%{municipio.strip()}%"

    if cnae and cnae.isdigit():
        clauses.append("cnae_fiscal_principal = :cnae")
        parameters["cnae"] = cnae

    if not clauses:
        return "", parameters

    return f"WHERE {' AND '.join(clauses)}", parameters


async def _fetch_company_search_page(
    database: DatabaseManager,
    request: Request,
    table_name: str,
    q: str | None,
    uf: str | None,
    municipio: str | None,
    cnae: str | None,
    page: int,
    page_size: int,
) -> PaginatedCompanySearchResponse:
    where_clause, parameters = _build_company_search_filters(q, uf, municipio, cnae)
    offset = (page - 1) * page_size

    count_row = await database.fetch_one(
        f"""
        SELECT COUNT(*) AS total
        FROM {table_name}
        {where_clause}
        """,
        parameters,
    )
    total = int((count_row or {}).get("total", 0))

    rows = await database.fetch_all(
        f"""
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
            reference_month
        FROM {table_name}
        {where_clause}
        ORDER BY razao_social
        LIMIT :limit OFFSET :offset
        """,
        {
            **parameters,
            "limit": page_size,
            "offset": offset,
        },
    )

    next_url = None
    previous_url = None
    if offset + page_size < total:
        next_url = _build_page_url(request, page + 1, page_size)
    if page > 1:
        previous_url = _build_page_url(request, page - 1, page_size)

    return PaginatedCompanySearchResponse(
        count=total,
        next=next_url,
        previous=previous_url,
        results=[_normalize_company_search_row(row) for row in rows],
    )


async def _fetch_company_search_detail(
    database: DatabaseManager,
    table_name: str,
    cnpj_14: str,
) -> CompanySearchItem:
    if not (cnpj_14.isdigit() and len(cnpj_14) == 14):
        raise HTTPException(status_code=422, detail="Informe um CNPJ válido com 14 dígitos numéricos.")

    row = await database.fetch_one(
        f"""
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
            reference_month
        FROM {table_name}
        WHERE cnpj_14 = :cnpj_14
        LIMIT 1
        """,
        {"cnpj_14": cnpj_14},
    )

    if row is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada para o CNPJ informado.")

    return _normalize_company_search_row(row)


async def _fetch_company_detail(
    database: DatabaseManager,
    cnpj_basico: str,
) -> CompanyDetailResponse:
    if not (cnpj_basico.isdigit() and len(cnpj_basico) == 8):
        raise HTTPException(status_code=422, detail="Informe um CNPJ básico válido com 8 dígitos numéricos.")

    company = await database.fetch_one(
        """
        SELECT
            empresa.cnpj_basico,
            empresa.razao_social,
            empresa.natureza_juridica,
            natureza.descricao AS natureza_juridica_descricao,
            empresa.qualificacao_responsavel,
            qualificacao.descricao AS qualificacao_responsavel_descricao,
            empresa.capital_social,
            empresa.porte_empresa,
            empresa.ente_federativo_responsavel,
            COALESCE(stats.total_estabelecimentos, 0) AS total_estabelecimentos,
            COALESCE(stats.estabelecimentos_ativos, 0) AS estabelecimentos_ativos
        FROM cnpj.empresa AS empresa
        LEFT JOIN cnpj.natureza_juridica AS natureza
            ON natureza.codigo = empresa.natureza_juridica
        LEFT JOIN cnpj.qualificacao_socio AS qualificacao
            ON qualificacao.codigo = empresa.qualificacao_responsavel
        LEFT JOIN (
            SELECT
                cnpj_basico,
                COUNT(*) AS total_estabelecimentos,
                COUNT(*) FILTER (WHERE situacao_cadastral = '02') AS estabelecimentos_ativos
            FROM cnpj.estabelecimento
            WHERE cnpj_basico = :cnpj_basico
            GROUP BY cnpj_basico
        ) AS stats
            ON stats.cnpj_basico = empresa.cnpj_basico
        WHERE empresa.cnpj_basico = :cnpj_basico
        LIMIT 1
        """,
        {"cnpj_basico": cnpj_basico},
    )

    if company is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada para o CNPJ informado.")

    establishment_rows = await database.fetch_all(
        """
        SELECT
            estabelecimento.cnpj_ordem,
            estabelecimento.cnpj_dv,
            estabelecimento.identificador_matriz_filial,
            estabelecimento.nome_fantasia,
            estabelecimento.situacao_cadastral,
            estabelecimento.data_situacao_cadastral,
            estabelecimento.motivo_situacao_cadastral,
            motivo.descricao AS motivo_situacao_cadastral_descricao,
            estabelecimento.data_inicio_atividade,
            estabelecimento.cnae_fiscal_principal,
            cnae.descricao AS cnae_fiscal_principal_descricao,
            estabelecimento.cnae_fiscal_secundaria,
            estabelecimento.codigo_pais AS pais,
            pais.nome AS pais_nome,
            estabelecimento.tipo_logradouro,
            estabelecimento.logradouro,
            estabelecimento.numero,
            estabelecimento.complemento,
            estabelecimento.bairro,
            estabelecimento.cep,
            estabelecimento.uf,
            estabelecimento.codigo_municipio AS municipio,
            municipio.nome AS municipio_nome,
            estabelecimento.ddd_1 AS ddd_telefone_1,
            estabelecimento.telefone_1,
            estabelecimento.ddd_2 AS ddd_telefone_2,
            estabelecimento.telefone_2,
            estabelecimento.ddd_fax,
            estabelecimento.fax,
            estabelecimento.correio_eletronico
        FROM cnpj.estabelecimento AS estabelecimento
        LEFT JOIN cnpj.cnae AS cnae
            ON cnae.cnae_fiscal = estabelecimento.cnae_fiscal_principal
        LEFT JOIN cnpj.motivo_situacao_cadastral AS motivo
            ON motivo.codigo = estabelecimento.motivo_situacao_cadastral
        LEFT JOIN cnpj.pais AS pais
            ON pais.codigo = estabelecimento.codigo_pais
        LEFT JOIN cnpj.municipio AS municipio
            ON municipio.codigo = estabelecimento.codigo_municipio
        WHERE estabelecimento.cnpj_basico = :cnpj_basico
        ORDER BY estabelecimento.identificador_matriz_filial NULLS LAST, estabelecimento.cnpj_ordem, estabelecimento.cnpj_dv
        """,
        {"cnpj_basico": cnpj_basico},
    )

    establishments = []
    for row in establishment_rows:
        full_cnpj = f"{cnpj_basico}{row['cnpj_ordem']}{row['cnpj_dv']}"
        formatted_cnpj = f"{full_cnpj[:2]}.{full_cnpj[2:5]}.{full_cnpj[5:8]}/{full_cnpj[8:12]}-{full_cnpj[12:14]}"
        establishments.append(
            CompanyEstablishment(
                cnpj_completo=formatted_cnpj,
                cnpj_ordem=row["cnpj_ordem"],
                cnpj_dv=row["cnpj_dv"],
                identificador_matriz_filial=row["identificador_matriz_filial"],
                tipo_estabelecimento=MATRIZ_FILIAL_LABELS.get(row["identificador_matriz_filial"]),
                nome_fantasia=row["nome_fantasia"],
                situacao_cadastral=row["situacao_cadastral"],
                situacao_cadastral_display=SITUACAO_CADASTRAL_LABELS.get(row["situacao_cadastral"]),
                data_situacao_cadastral=row["data_situacao_cadastral"],
                motivo_situacao_cadastral=row["motivo_situacao_cadastral"],
                motivo_situacao_cadastral_descricao=row["motivo_situacao_cadastral_descricao"],
                data_inicio_atividade=row["data_inicio_atividade"],
                cnae_fiscal_principal=row["cnae_fiscal_principal"],
                cnae_fiscal_principal_descricao=row["cnae_fiscal_principal_descricao"],
                cnae_fiscal_secundaria=row["cnae_fiscal_secundaria"],
                pais=row["pais"],
                pais_nome=row["pais_nome"],
                endereco=EstablishmentAddress(
                    tipo_logradouro=row["tipo_logradouro"],
                    logradouro=row["logradouro"],
                    numero=row["numero"],
                    complemento=row["complemento"],
                    bairro=row["bairro"],
                    cep=row["cep"],
                    uf=row["uf"],
                    municipio=row["municipio"],
                    municipio_nome=row["municipio_nome"],
                ),
                contato=EstablishmentContact(
                    ddd_telefone_1=_format_phone(row["ddd_telefone_1"], row["telefone_1"]),
                    ddd_telefone_2=_format_phone(row["ddd_telefone_2"], row["telefone_2"]),
                    ddd_fax=_format_phone(row["ddd_fax"], row["fax"]),
                    email=row["correio_eletronico"],
                ),
            )
        )

    return CompanyDetailResponse(
        cnpj_basico=company["cnpj_basico"],
        razao_social=company["razao_social"],
        natureza_juridica=company["natureza_juridica"],
        natureza_juridica_descricao=company["natureza_juridica_descricao"],
        qualificacao_responsavel=company["qualificacao_responsavel"],
        qualificacao_responsavel_descricao=company["qualificacao_responsavel_descricao"],
        capital_social=company["capital_social"],
        porte_empresa=company["porte_empresa"],
        porte_empresa_display=PORTE_EMPRESA_LABELS.get(company["porte_empresa"]),
        ente_federativo_responsavel=company["ente_federativo_responsavel"],
        total_estabelecimentos=int(company["total_estabelecimentos"]),
        estabelecimentos_ativos=int(company["estabelecimentos_ativos"]),
        estabelecimentos=establishments,
    )


async def _fetch_person_search_page(
    database: DatabaseManager,
    request: Request,
    q: str,
    page: int,
    page_size: int,
) -> PaginatedPersonSearchResponse:
    if len(q.strip()) < 3:
        raise HTTPException(status_code=422, detail="Informe ao menos 3 caracteres para busca por nome.")

    normalized_query = f"%{_strip_accents(q.strip())}%"
    offset = (page - 1) * page_size
    params = {
        "nome": normalized_query,
        "limit": page_size,
        "offset": offset,
    }

    count_row = await database.fetch_one(
        """
        SELECT COUNT(*) AS total
        FROM (
            SELECT 1
            FROM cnpj.socio
            WHERE identificador_socio IN (2, 3)
              AND nome_socio_razao_social ILIKE :nome
            GROUP BY nome_socio_razao_social, cpf_cnpj_socio, faixa_etaria
        ) AS grouped_people
        """,
        {"nome": normalized_query},
    )
    total = int((count_row or {}).get("total", 0))

    rows = await database.fetch_all(
        """
        SELECT
            nome_socio_razao_social AS nome,
            cpf_cnpj_socio,
            faixa_etaria,
            COUNT(DISTINCT cnpj_basico) AS total_empresas
        FROM cnpj.socio
        WHERE identificador_socio IN (2, 3)
          AND nome_socio_razao_social ILIKE :nome
        GROUP BY nome_socio_razao_social, cpf_cnpj_socio, faixa_etaria
        ORDER BY nome_socio_razao_social
        LIMIT :limit OFFSET :offset
        """,
        params,
    )

    next_url = None
    previous_url = None
    if offset + page_size < total:
        next_url = _build_page_url(request, page + 1, page_size)
    if page > 1:
        previous_url = _build_page_url(request, page - 1, page_size)

    return PaginatedPersonSearchResponse(
        count=total,
        next=next_url,
        previous=previous_url,
        results=[_normalize_person_search_row(row) for row in rows],
    )


async def _fetch_person_detail(
    database: DatabaseManager,
    nome: str,
    cpf_masked: str,
) -> PersonDetailResponse:
    normalized_name = _strip_accents(nome.strip())
    if len(normalized_name) < 3:
        raise HTTPException(
            status_code=422,
            detail="Informe o nome da pessoa com ao menos 3 caracteres.",
        )

    rows = await database.fetch_all(
        """
        SELECT
            socio.nome_socio_razao_social AS nome,
            socio.cpf_cnpj_socio,
            socio.faixa_etaria,
            empresa.cnpj_basico,
            empresa.razao_social,
            socio.qualificacao_socio,
            qualificacao.descricao AS qualificacao_socio_descricao,
            socio.data_entrada_sociedade,
            matriz.situacao_cadastral,
            matriz.uf,
            municipio.nome AS municipio_nome,
            socio.reference_month
        FROM cnpj.socio AS socio
        INNER JOIN cnpj.empresa AS empresa
            ON empresa.cnpj_basico = socio.cnpj_basico
        LEFT JOIN cnpj.qualificacao_socio AS qualificacao
            ON qualificacao.codigo = socio.qualificacao_socio
        LEFT JOIN cnpj.estabelecimento AS matriz
            ON matriz.cnpj_basico = empresa.cnpj_basico
           AND matriz.identificador_matriz_filial = 1
        LEFT JOIN cnpj.municipio AS municipio
            ON municipio.codigo = matriz.codigo_municipio
        WHERE socio.cpf_cnpj_socio = :cpf_masked
          AND socio.identificador_socio IN (2, 3)
          AND socio.nome_socio_razao_social = :nome
        ORDER BY empresa.razao_social, socio.reference_month DESC
        """,
        {
            "cpf_masked": cpf_masked,
            "nome": normalized_name,
        },
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="Pessoa não encontrada para o par nome + CPF mascarado informado.",
        )

    first = rows[0]
    seen_companies: dict[str, PersonCompanyItem] = {}
    for row in rows:
        cnpj_basico = row["cnpj_basico"]
        if cnpj_basico in seen_companies:
            continue
        seen_companies[cnpj_basico] = PersonCompanyItem(
            cnpj_basico=cnpj_basico,
            razao_social=row["razao_social"],
            qualificacao_socio=row["qualificacao_socio"],
            qualificacao_socio_descricao=row["qualificacao_socio_descricao"],
            data_entrada_sociedade=row["data_entrada_sociedade"],
            situacao_cadastral=row["situacao_cadastral"],
            uf=row["uf"],
            municipio_nome=row["municipio_nome"],
            reference_month=row["reference_month"],
        )

    return PersonDetailResponse(
        cpf_mascarado=cpf_masked,
        nome=first["nome"],
        faixa_etaria=first["faixa_etaria"],
        total_empresas=len(seen_companies),
        empresas=list(seen_companies.values()),
    )


async def _fetch_company_network(
    database: DatabaseManager,
    cnpj_basico: str,
    depth: int,
) -> CompanyNetworkResponse:
    if not (cnpj_basico.isdigit() and len(cnpj_basico) == 8):
        raise HTTPException(status_code=422, detail="Informe um CNPJ básico válido com 8 dígitos numéricos.")

    service = CnpjNetworkService(database)
    try:
        payload = await service.get_company_network(cnpj_basico, depth=depth)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="Falha ao consultar rede no Neo4j.") from exc

    if payload is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada na base Neo4j para o CNPJ informado.")

    return CompanyNetworkResponse.model_validate(payload)


async def _fetch_person_network(
    database: DatabaseManager,
    nome: str,
    cpf_masked: str,
    depth: int,
) -> PersonNetworkResponse:
    normalized_name = _strip_accents(nome.strip())
    if len(normalized_name) < 3:
        raise HTTPException(status_code=422, detail="Informe o nome da pessoa com ao menos 3 caracteres.")

    service = CnpjNetworkService(database)
    try:
        payload = await service.get_person_network(cpf_masked, nome=normalized_name, depth=depth)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="Falha ao consultar rede no Neo4j.") from exc

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail="Pessoa não encontrada na base Neo4j para o par nome + CPF mascarado informado.",
        )

    return PersonNetworkResponse.model_validate(payload)


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", response_model=CnpjBootstrapStatus, summary="CNPJ migration status")
    async def cnpj_bootstrap_status() -> CnpjBootstrapStatus:
        return CnpjBootstrapStatus(
            domain="cnpj",
            status="implemented",
            implemented_endpoints=IMPLEMENTED_ENDPOINTS,
            pending_endpoints=PENDING_ENDPOINTS,
        )

    @router.get("/search", response_model=PaginatedCompanySearchResponse, summary="Search active companies")
    async def search_companies(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        municipio: str | None = Query(default=None),
        cnae: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedCompanySearchResponse:
        return await _fetch_company_search_page(
            database=database,
            request=request,
            table_name="cnpj.mv_company_search",
            q=q,
            uf=uf,
            municipio=municipio,
            cnae=cnae,
            page=page,
            page_size=page_size,
        )

    @router.get("/search/{cnpj_14}", response_model=CompanySearchItem, summary="Get active company by CNPJ")
    async def search_company_detail(
        cnpj_14: str,
        database: DatabaseManager = Depends(get_database_manager),
    ) -> CompanySearchItem:
        return await _fetch_company_search_detail(database, "cnpj.mv_company_search", cnpj_14)

    @router.get(
        "/search-inactive",
        response_model=PaginatedCompanySearchResponse,
        summary="Search inactive companies",
    )
    async def search_inactive_companies(
        request: Request,
        q: str | None = Query(default=None),
        uf: str | None = Query(default=None),
        municipio: str | None = Query(default=None),
        cnae: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedCompanySearchResponse:
        return await _fetch_company_search_page(
            database=database,
            request=request,
            table_name="cnpj.mv_company_search_inactive",
            q=q,
            uf=uf,
            municipio=municipio,
            cnae=cnae,
            page=page,
            page_size=page_size,
        )

    @router.get(
        "/search-inactive/{cnpj_14}",
        response_model=CompanySearchItem,
        summary="Get inactive company by CNPJ",
    )
    async def search_inactive_company_detail(
        cnpj_14: str,
        database: DatabaseManager = Depends(get_database_manager),
    ) -> CompanySearchItem:
        return await _fetch_company_search_detail(database, "cnpj.mv_company_search_inactive", cnpj_14)

    @router.get("/empresa/{cnpj_basico}", response_model=CompanyDetailResponse, summary="Get company detail")
    async def get_company_detail(
        cnpj_basico: str,
        database: DatabaseManager = Depends(get_database_manager),
    ) -> CompanyDetailResponse:
        return await _fetch_company_detail(database, cnpj_basico)

    @router.get(
        "/pessoa/search",
        response_model=PaginatedPersonSearchResponse,
        summary="Search people in the ownership dataset",
    )
    async def search_people(
        request: Request,
        q: str = Query(...),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedPersonSearchResponse:
        return await _fetch_person_search_page(database, request, q, page, page_size)

    @router.get(
        "/pessoa/detail",
        response_model=PersonDetailResponse,
        summary="Get person detail by name and masked CPF",
    )
    async def get_person_detail(
        nome: str = Query(...),
        cpf_mascarado: str = Query(...),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PersonDetailResponse:
        normalized_cpf = _normalize_masked_cpf(cpf_mascarado)
        if normalized_cpf is None:
            raise HTTPException(
                status_code=422,
                detail="Informe um CPF mascarado válido no formato ***123456**.",
            )
        return await _fetch_person_detail(database, nome, normalized_cpf)

    @router.get(
        "/pessoa/{cpf}",
        response_model=PersonDetailResponse,
        summary="Get person detail by CPF plus name",
    )
    async def get_person_detail_by_cpf(
        cpf: str,
        nome: str = Query(...),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PersonDetailResponse:
        cpf_masked = _mask_cpf(cpf)
        if cpf_masked is None:
            raise HTTPException(
                status_code=422,
                detail="Informe um CPF válido com 11 dígitos numéricos.",
            )
        return await _fetch_person_detail(database, nome, cpf_masked)

    @router.get(
        "/network/{cnpj_basico}",
        response_model=CompanyNetworkResponse,
        summary="Get company ownership network",
    )
    async def get_company_network(
        cnpj_basico: str,
        depth: int = Query(default=1, ge=1),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> CompanyNetworkResponse:
        max_depth = max(1, database.settings.neo4j_max_network_depth)
        if depth > max_depth:
            raise HTTPException(status_code=422, detail=f"Informe uma profundidade entre 1 e {max_depth}.")
        return await _fetch_company_network(database, cnpj_basico, depth)

    @router.get(
        "/pessoa-network/detail",
        response_model=PersonNetworkResponse,
        summary="Get person ownership network by name and masked CPF",
    )
    async def get_person_network_detail(
        nome: str = Query(...),
        cpf_mascarado: str = Query(...),
        depth: int = Query(default=1, ge=1),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PersonNetworkResponse:
        normalized_cpf = _normalize_masked_cpf(cpf_mascarado)
        if normalized_cpf is None:
            raise HTTPException(
                status_code=422,
                detail="Informe um CPF mascarado válido no formato ***123456**.",
            )
        max_depth = max(1, database.settings.neo4j_max_network_depth)
        if depth > max_depth:
            raise HTTPException(status_code=422, detail=f"Informe uma profundidade entre 1 e {max_depth}.")
        return await _fetch_person_network(database, nome, normalized_cpf, depth)

    @router.get(
        "/pessoa-network/{cpf}",
        response_model=PersonNetworkResponse,
        summary="Get person ownership network by CPF plus name",
    )
    async def get_person_network_by_cpf(
        cpf: str,
        nome: str = Query(...),
        depth: int = Query(default=1, ge=1),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PersonNetworkResponse:
        cpf_masked = _mask_cpf(cpf)
        if cpf_masked is None:
            raise HTTPException(
                status_code=422,
                detail="Informe um CPF válido com 11 dígitos numéricos.",
            )
        max_depth = max(1, database.settings.neo4j_max_network_depth)
        if depth > max_depth:
            raise HTTPException(status_code=422, detail=f"Informe uma profundidade entre 1 e {max_depth}.")
        return await _fetch_person_network(database, nome, cpf_masked, depth)

    return router
