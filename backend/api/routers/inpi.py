"""INPI FastAPI router backed by async Postgres queries."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.schemas.inpi import (
    InpiBootstrapStatus,
    InpiCompanyLookup,
    InpiRecord,
    InpiResumoAnoItem,
    InpiResumoIPCItem,
    InpiResumoResponse,
    InpiResumoTipoItem,
    PaginatedInpiRecordResponse,
)
from core.database import DatabaseManager, get_database_manager

PAGE_SIZE_DEFAULT = 20
PAGE_SIZE_MAX = 100

IMPLEMENTED_ENDPOINTS = [
    "/patentes",
    "/patentes/{codigo_interno}",
    "/inventores",
    "/depositantes",
    "/classificacao-ipc",
    "/despachos",
    "/procuradores",
    "/prioridades",
    "/vinculos",
    "/renumeracoes",
    "/resumo",
    "/ipc-ref",
]

PENDING_ENDPOINTS: list[str] = []


def _digits_only(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _build_page_url(request: Request, page: int, page_size: int) -> str:
    return str(request.url.include_query_params(page=page, page_size=page_size))


def _paginate_items(
    request: Request,
    items: list[InpiRecord],
    page: int,
    page_size: int,
) -> PaginatedInpiRecordResponse:
    total = len(items)
    offset = (page - 1) * page_size
    page_items = items[offset : offset + page_size]
    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return PaginatedInpiRecordResponse(count=total, next=next_url, previous=previous_url, results=page_items)


async def _fetch_company_lookup_by_cnpj_basico(
    database: DatabaseManager,
    cnpj_basicos: set[str],
) -> dict[str, InpiCompanyLookup]:
    if not cnpj_basicos:
        return {}

    lookup: dict[str, InpiCompanyLookup] = {}
    active_rows = await database.fetch_all(
        """
        SELECT
            cnpj_14,
            cnpj_basico,
            razao_social,
            nome_fantasia,
            situacao_cadastral,
            municipio_nome,
            uf,
            cnae_fiscal_principal,
            cnae_descricao,
            porte_empresa,
            natureza_juridica,
            natureza_juridica_descricao,
            capital_social,
            data_inicio_atividade,
            reference_month,
            'active' AS match_source
        FROM cnpj.mv_company_search
        WHERE cnpj_basico = ANY(:cnpj_basicos)
        ORDER BY cnpj_basico, cnpj_14
        """,
        {"cnpj_basicos": list(cnpj_basicos)},
    )
    for row in active_rows:
        basico = row["cnpj_basico"]
        existing = lookup.get(basico)
        is_matriz = row["cnpj_14"][8:12] == "0001"
        if existing is None or is_matriz:
            lookup[basico] = InpiCompanyLookup.model_validate(row)

    missing = sorted(cnpj_basicos.difference(lookup.keys()))
    if missing:
        inactive_rows = await database.fetch_all(
            """
            SELECT
                cnpj_14,
                cnpj_basico,
                razao_social,
                nome_fantasia,
                situacao_cadastral,
                municipio_nome,
                uf,
                cnae_fiscal_principal,
                cnae_descricao,
                porte_empresa,
                natureza_juridica,
                natureza_juridica_descricao,
                capital_social,
                data_inicio_atividade,
                reference_month,
                'inactive' AS match_source
            FROM cnpj.mv_company_search_inactive
            WHERE cnpj_basico = ANY(:cnpj_basicos)
            ORDER BY cnpj_basico, cnpj_14
            """,
            {"cnpj_basicos": missing},
        )
        for row in inactive_rows:
            basico = row["cnpj_basico"]
            existing = lookup.get(basico)
            is_matriz = row["cnpj_14"][8:12] == "0001"
            if existing is None or is_matriz:
                lookup[basico] = InpiCompanyLookup.model_validate(row)

    return lookup


async def _enrich_company_join(
    database: DatabaseManager,
    rows: list[dict[str, Any]],
    field_name: str,
    payload_name: str,
) -> list[InpiRecord]:
    basicos = {
        row.get(field_name)
        for row in rows
        if row.get(field_name) and len(str(row.get(field_name))) == 8
    }
    lookup = await _fetch_company_lookup_by_cnpj_basico(database, basicos)
    payloads: list[InpiRecord] = []
    for row in rows:
        item = dict(row)
        item[payload_name] = lookup.get(item.get(field_name))
        payloads.append(InpiRecord.model_validate(item))
    return payloads


def _build_mv_patent_filters(
    q: str | None,
    tipo_patente: str | None,
    ipc_secao: str | None,
    cnpj: str | None,
    numero_inpi: str | None,
    data_deposito_min: str | None,
    data_deposito_max: str | None,
    sigilo: str | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if q:
        clauses.append(
            "(titulo ILIKE :q OR depositante_principal ILIKE :q OR inventor_principal ILIKE :q)"
        )
        params["q"] = f"%{q.strip()}%"
    if tipo_patente:
        clauses.append("tipo_patente = :tipo_patente")
        params["tipo_patente"] = tipo_patente.strip().upper()
    if ipc_secao:
        clauses.append("ipc_secao_classe ILIKE :ipc_secao")
        params["ipc_secao"] = f"{ipc_secao.strip().upper()}%"
    if cnpj:
        digits = _digits_only(cnpj)
        if len(digits) >= 8:
            clauses.append("depositante_cnpj_basico = :cnpj_basico")
            params["cnpj_basico"] = digits[:8]
    if numero_inpi:
        clauses.append("numero_inpi = :numero_inpi")
        params["numero_inpi"] = numero_inpi.strip()
    if data_deposito_min:
        clauses.append("data_deposito >= :data_deposito_min")
        params["data_deposito_min"] = data_deposito_min.strip()
    if data_deposito_max:
        clauses.append("data_deposito <= :data_deposito_max")
        params["data_deposito_max"] = data_deposito_max.strip()
    sigilo_value = (sigilo or "").strip().lower()
    if sigilo_value in {"true", "1", "yes"}:
        clauses.append("sigilo = true")
    elif sigilo_value in {"false", "0", "no"}:
        clauses.append("sigilo = false")
    if not clauses:
        return "", params
    return f"WHERE {' AND '.join(clauses)}", params


async def _list_patentes(
    database: DatabaseManager,
    request: Request,
    q: str | None,
    tipo_patente: str | None,
    ipc_secao: str | None,
    cnpj: str | None,
    numero_inpi: str | None,
    data_deposito_min: str | None,
    data_deposito_max: str | None,
    sigilo: str | None,
    page: int,
    page_size: int,
) -> PaginatedInpiRecordResponse:
    where_clause, params = _build_mv_patent_filters(
        q,
        tipo_patente,
        ipc_secao,
        cnpj,
        numero_inpi,
        data_deposito_min,
        data_deposito_max,
        sigilo,
    )
    total_row = await database.fetch_one(
        f"SELECT COUNT(*) AS total FROM inpi.mv_patent_search {where_clause}",
        params,
    )
    total = int((total_row or {}).get("total", 0))
    offset = (page - 1) * page_size
    rows = await database.fetch_all(
        f"""
        SELECT *
        FROM inpi.mv_patent_search
        {where_clause}
        ORDER BY data_deposito DESC NULLS LAST, numero_inpi ASC
        LIMIT :limit OFFSET :offset
        """,
        {**params, "limit": page_size, "offset": offset},
    )
    payloads = await _enrich_company_join(database, rows, "depositante_cnpj_basico", "depositante_empresa")
    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return PaginatedInpiRecordResponse(count=total, next=next_url, previous=previous_url, results=payloads)


async def _get_patente(database: DatabaseManager, codigo_interno: int) -> InpiRecord:
    row = await database.fetch_one(
        "SELECT * FROM inpi.mv_patent_search WHERE codigo_interno = :codigo_interno LIMIT 1",
        {"codigo_interno": codigo_interno},
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Patente não encontrada.")
    return (await _enrich_company_join(database, [row], "depositante_cnpj_basico", "depositante_empresa"))[0]


async def _list_subtable(
    database: DatabaseManager,
    request: Request,
    table: str,
    order_by: str,
    page: int,
    page_size: int,
    filters: list[tuple[str, Any]],
    company_field: str | None = None,
    company_payload_name: str | None = None,
) -> PaginatedInpiRecordResponse:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for clause, values in filters:
        if clause:
            clauses.append(clause)
        params.update(values)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    total_row = await database.fetch_one(f"SELECT COUNT(*) AS total FROM {table} {where_clause}", params)
    total = int((total_row or {}).get("total", 0))
    offset = (page - 1) * page_size
    rows = await database.fetch_all(
        f"SELECT * FROM {table} {where_clause} ORDER BY {order_by} LIMIT :limit OFFSET :offset",
        {**params, "limit": page_size, "offset": offset},
    )
    if company_field and company_payload_name:
        payloads = await _enrich_company_join(database, rows, company_field, company_payload_name)
    else:
        payloads = [InpiRecord.model_validate(row) for row in rows]
    next_url = _build_page_url(request, page + 1, page_size) if offset + page_size < total else None
    previous_url = _build_page_url(request, page - 1, page_size) if page > 1 else None
    return PaginatedInpiRecordResponse(count=total, next=next_url, previous=previous_url, results=payloads)


async def _get_resumo(
    database: DatabaseManager,
    q: str | None,
    tipo_patente: str | None,
    ipc_secao: str | None,
    cnpj: str | None,
    numero_inpi: str | None,
    data_deposito_min: str | None,
    data_deposito_max: str | None,
    sigilo: str | None,
) -> InpiResumoResponse:
    where_clause, params = _build_mv_patent_filters(
        q,
        tipo_patente,
        ipc_secao,
        cnpj,
        numero_inpi,
        data_deposito_min,
        data_deposito_max,
        sigilo,
    )
    total_row = await database.fetch_one(
        f"SELECT COUNT(*) AS total FROM inpi.mv_patent_search {where_clause}",
        params,
    )
    por_tipo_rows = await database.fetch_all(
        f"""
        SELECT tipo_patente, COUNT(codigo_interno) AS total
        FROM inpi.mv_patent_search
        {where_clause}
        GROUP BY tipo_patente
        ORDER BY total DESC
        """,
        params,
    )
    por_ipc_rows = await database.fetch_all(
        f"""
        SELECT ipc_secao_classe, COUNT(codigo_interno) AS total
        FROM inpi.mv_patent_search
        {where_clause}{' AND ' if where_clause else 'WHERE '}ipc_secao_classe IS NOT NULL
        GROUP BY ipc_secao_classe
        ORDER BY total DESC
        LIMIT 50
        """,
        params,
    )
    ipc_codes = [row["ipc_secao_classe"] for row in por_ipc_rows if row.get("ipc_secao_classe")]
    ipc_lookup_rows = await database.fetch_all(
        "SELECT simbolo, titulo_en FROM inpi.ipc_subclasse_ref WHERE simbolo = ANY(:simbolos)",
        {"simbolos": ipc_codes or [""]},
    )
    ipc_lookup = {row["simbolo"]: row["titulo_en"] for row in ipc_lookup_rows}
    por_ano_rows = await database.fetch_all(
        f"""
        SELECT EXTRACT(YEAR FROM data_deposito)::int AS ano, COUNT(codigo_interno) AS total
        FROM inpi.mv_patent_search
        {where_clause}{' AND ' if where_clause else 'WHERE '}data_deposito IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM data_deposito)
        ORDER BY ano
        """,
        params,
    )
    snapshot_row = await database.fetch_one(
        f"SELECT snapshot_date FROM inpi.mv_patent_search {where_clause} ORDER BY snapshot_date DESC NULLS LAST LIMIT 1",
        params,
    )
    return InpiResumoResponse(
        total_patentes=int((total_row or {}).get("total", 0)),
        por_tipo=[InpiResumoTipoItem.model_validate(row) for row in por_tipo_rows],
        por_secao_ipc=[
            InpiResumoIPCItem(ipc_secao_classe=row["ipc_secao_classe"], total=row["total"], titulo_en=ipc_lookup.get(row["ipc_secao_classe"]))
            for row in por_ipc_rows
        ],
        por_ano_deposito=[InpiResumoAnoItem.model_validate(row) for row in por_ano_rows],
        snapshot_date=str(snapshot_row["snapshot_date"]) if snapshot_row and snapshot_row.get("snapshot_date") else None,
    )


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", response_model=InpiBootstrapStatus, summary="INPI migration status")
    async def inpi_bootstrap_status() -> InpiBootstrapStatus:
        return InpiBootstrapStatus(
            domain="inpi",
            status="implemented",
            implemented_endpoints=IMPLEMENTED_ENDPOINTS,
            pending_endpoints=PENDING_ENDPOINTS,
        )

    @router.get("/patentes", response_model=PaginatedInpiRecordResponse, summary="Search patents")
    async def list_patentes(
        request: Request,
        q: str | None = Query(default=None),
        tipo_patente: str | None = Query(default=None),
        ipc_secao: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        numero_inpi: str | None = Query(default=None),
        data_deposito_min: str | None = Query(default=None),
        data_deposito_max: str | None = Query(default=None),
        sigilo: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        return await _list_patentes(
            database,
            request,
            q,
            tipo_patente,
            ipc_secao,
            cnpj,
            numero_inpi,
            data_deposito_min,
            data_deposito_max,
            sigilo,
            page,
            page_size,
        )

    @router.get("/patentes/{codigo_interno}", response_model=InpiRecord, summary="Get patent by internal code")
    async def get_patente(
        codigo_interno: int,
        database: DatabaseManager = Depends(get_database_manager),
    ) -> InpiRecord:
        return await _get_patente(database, codigo_interno)

    @router.get("/inventores", response_model=PaginatedInpiRecordResponse, summary="List inventors")
    async def list_inventores(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        pais: str | None = Query(default=None),
        q: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if pais:
            filters.append(("pais = :pais", {"pais": pais.strip().upper()}))
        if q:
            filters.append(("autor ILIKE :q", {"q": f"%{q.strip()}%"}))
        return await _list_subtable(database, request, "inpi.patentes_inventores", "codigo_interno ASC, ordem ASC", page, page_size, filters)

    @router.get("/depositantes", response_model=PaginatedInpiRecordResponse, summary="List depositors")
    async def list_depositantes(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        cnpj_basico: str | None = Query(default=None),
        q: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if q:
            filters.append(("depositante ILIKE :q", {"q": f"%{q.strip()}%"}))
        digits = _digits_only(cnpj_basico or "")
        if len(digits) >= 8:
            filters.append(("cnpj_basico_resolved = :cnpj_basico", {"cnpj_basico": digits[:8]}))
        return await _list_subtable(
            database,
            request,
            "inpi.patentes_depositantes",
            "codigo_interno ASC, ordem ASC",
            page,
            page_size,
            filters,
            company_field="cnpj_basico_resolved",
            company_payload_name="empresa",
        )

    @router.get("/classificacao-ipc", response_model=PaginatedInpiRecordResponse, summary="List IPC classifications")
    async def list_classificacao_ipc(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        simbolo: str | None = Query(default=None),
        versao: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if simbolo:
            filters.append(("simbolo ILIKE :simbolo", {"simbolo": f"{simbolo.strip().upper()}%"}))
        if versao:
            filters.append(("versao = :versao", {"versao": versao.strip()}))
        return await _list_subtable(database, request, "inpi.patentes_classificacao_ipc", "codigo_interno ASC, ordem ASC", page, page_size, filters)

    @router.get("/despachos", response_model=PaginatedInpiRecordResponse, summary="List patent dispatches")
    async def list_despachos(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        codigo_despacho: str | None = Query(default=None),
        data_rpi_min: str | None = Query(default=None),
        data_rpi_max: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if codigo_despacho:
            filters.append(("codigo_despacho = :codigo_despacho", {"codigo_despacho": codigo_despacho.strip()}))
        if data_rpi_min:
            filters.append(("data_rpi >= :data_rpi_min", {"data_rpi_min": data_rpi_min.strip()}))
        if data_rpi_max:
            filters.append(("data_rpi <= :data_rpi_max", {"data_rpi_max": data_rpi_max.strip()}))
        return await _list_subtable(database, request, "inpi.patentes_despachos", "codigo_interno ASC, data_rpi DESC NULLS LAST", page, page_size, filters)

    @router.get("/procuradores", response_model=PaginatedInpiRecordResponse, summary="List attorneys")
    async def list_procuradores(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        cnpj_basico: str | None = Query(default=None),
        q: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if q:
            filters.append(("procurador ILIKE :q", {"q": f"%{q.strip()}%"}))
        digits = _digits_only(cnpj_basico or "")
        if len(digits) >= 8:
            filters.append(("cnpj_basico_resolved = :cnpj_basico", {"cnpj_basico": digits[:8]}))
        return await _list_subtable(
            database,
            request,
            "inpi.patentes_procuradores",
            "codigo_interno ASC, id ASC",
            page,
            page_size,
            filters,
            company_field="cnpj_basico_resolved",
            company_payload_name="empresa",
        )

    @router.get("/prioridades", response_model=PaginatedInpiRecordResponse, summary="List patent priorities")
    async def list_prioridades(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        pais_prioridade: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if pais_prioridade:
            filters.append(("pais_prioridade = :pais_prioridade", {"pais_prioridade": pais_prioridade.strip().upper()}))
        return await _list_subtable(database, request, "inpi.patentes_prioridades", "codigo_interno ASC, pais_prioridade ASC", page, page_size, filters)

    @router.get("/vinculos", response_model=PaginatedInpiRecordResponse, summary="List patent links")
    async def list_vinculos(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        tipo_vinculo: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno_derivado = :codigo_interno", {"codigo_interno": codigo_interno}))
        if tipo_vinculo:
            filters.append(("tipo_vinculo = :tipo_vinculo", {"tipo_vinculo": tipo_vinculo.strip().upper()}))
        return await _list_subtable(database, request, "inpi.patentes_vinculos", "codigo_interno_derivado ASC, tipo_vinculo ASC", page, page_size, filters)

    @router.get("/renumeracoes", response_model=PaginatedInpiRecordResponse, summary="List patent renumberings")
    async def list_renumeracoes(
        request: Request,
        codigo_interno: int | None = Query(default=None),
        numero_inpi_original: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if codigo_interno is not None:
            filters.append(("codigo_interno = :codigo_interno", {"codigo_interno": codigo_interno}))
        if numero_inpi_original:
            filters.append(("numero_inpi_original = :numero_inpi_original", {"numero_inpi_original": numero_inpi_original.strip()}))
        return await _list_subtable(database, request, "inpi.patentes_renumeracoes", "codigo_interno ASC", page, page_size, filters)

    @router.get("/ipc-ref", response_model=PaginatedInpiRecordResponse, summary="List IPC subclass references")
    async def list_ipc_ref(
        request: Request,
        secao: str | None = Query(default=None),
        q: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=PAGE_SIZE_DEFAULT, ge=1, le=PAGE_SIZE_MAX),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> PaginatedInpiRecordResponse:
        filters = []
        if secao:
            filters.append(("secao = :secao", {"secao": secao.strip().upper()}))
        if q:
            filters.append(("(titulo_en ILIKE :q OR simbolo ILIKE :simbolo)", {"q": f"%{q.strip()}%", "simbolo": f"{q.strip().upper()}%"}))
        return await _list_subtable(database, request, "inpi.ipc_subclasse_ref", "simbolo ASC", page, page_size, filters)

    @router.get("/resumo", response_model=InpiResumoResponse, summary="Get INPI summary")
    async def get_resumo(
        q: str | None = Query(default=None),
        tipo_patente: str | None = Query(default=None),
        ipc_secao: str | None = Query(default=None),
        cnpj: str | None = Query(default=None),
        numero_inpi: str | None = Query(default=None),
        data_deposito_min: str | None = Query(default=None),
        data_deposito_max: str | None = Query(default=None),
        sigilo: str | None = Query(default=None),
        database: DatabaseManager = Depends(get_database_manager),
    ) -> InpiResumoResponse:
        return await _get_resumo(
            database,
            q,
            tipo_patente,
            ipc_secao,
            cnpj,
            numero_inpi,
            data_deposito_min,
            data_deposito_max,
            sigilo,
        )

    return router
