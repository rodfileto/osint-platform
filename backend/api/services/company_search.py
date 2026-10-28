"""Company search over cnpj.company_search, one is_active half at a time.

The trigram and name-order indexes are partial on is_active, so every query names the
predicate literally (a bind parameter would keep generic plans off them). A page is found
in up to three steps, cheapest first:

1. Probe up to COUNT_CAP + 1 matches in any order: a trigram bitmap for rare terms, a scan
   that stops early for common ones. Its size is also the (capped) count.
2. At most COUNT_CAP matches: sort those and take the page.
3. More: walk the first NAME_WALK names in order and filter them. If that fills the page it
   is exactly the first page, since every other row sorts later. Otherwise (many matches,
   clustered late in the alphabet) sort all matches.

Left to itself the planner, misjudging how many rows a name pattern matches, either sorted
millions of matches ("SILVA": 25 s) or walked the name index past a million non-matches.
"""

from typing import Any

from core.database import DatabaseManager

COUNT_CAP = 10_000
NAME_WALK = 50_000

COLUMNS = """
    cnpj_14, cnpj_basico, razao_social, nome_fantasia, situacao_cadastral, codigo_municipio,
    municipio_nome, uf, cnae_fiscal_principal, cnae_descricao, porte_empresa, natureza_juridica,
    natureza_juridica_descricao, capital_social, data_inicio_atividade, correio_eletronico,
    reference_month
"""

ORDER = "razao_social, cnpj_14"


async def search_half(
    database: DatabaseManager,
    is_active: bool,
    match: str,
    params: dict[str, Any],
    limit: int,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int, bool]:
    """One page of matches in name order, the match count up to COUNT_CAP, and whether more match."""
    active = "is_active" if is_active else "NOT is_active"
    where = f"{active} AND ({match})" if match else active
    page = {"limit": limit, "offset": offset}

    probe = await database.fetch_one(
        f"""
        WITH probe AS MATERIALIZED (
            SELECT cnpj_14, razao_social FROM cnpj.company_search WHERE {where} LIMIT :probe_limit
        )
        SELECT
            (SELECT count(*) FROM probe) AS total,
            array(SELECT cnpj_14 FROM probe ORDER BY {ORDER} LIMIT :limit OFFSET :offset) AS ids
        """,
        {**params, **page, "probe_limit": COUNT_CAP + 1},
    )
    total = int(probe["total"])
    if total <= COUNT_CAP:
        if not probe["ids"]:
            return [], total, False
        rows = await database.fetch_all(
            f"SELECT {COLUMNS} FROM cnpj.company_search WHERE cnpj_14 = ANY(:ids) ORDER BY {ORDER}",
            {"ids": list(probe["ids"])},
        )
        return rows, total, False

    rows = await database.fetch_all(
        f"""
        SELECT {COLUMNS}
        FROM (SELECT * FROM cnpj.company_search WHERE {active} ORDER BY {ORDER} LIMIT :walk) AS first_names
        WHERE {match or "TRUE"}
        ORDER BY {ORDER}
        LIMIT :limit OFFSET :offset
        """,
        {**params, **page, "walk": NAME_WALK},
    )
    if len(rows) < limit:
        rows = await database.fetch_all(
            f"""
            WITH matches AS MATERIALIZED (SELECT {COLUMNS} FROM cnpj.company_search WHERE {where})
            SELECT * FROM matches ORDER BY {ORDER} LIMIT :limit OFFSET :offset
            """,
            {**params, **page},
        )
    return rows, COUNT_CAP, True
