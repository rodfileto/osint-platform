"""CNPJ ownership networks built from cnpj.socio in PostgreSQL.

A network is expanded breadth-first, one level per hop: for the companies reached so far, their
partners and the companies they are partners in; for the people reached so far, the companies they
are partners in. Every lookup hits an index (idx_socio_cnpj_basico, idx_socio_pj_cnpj_basico,
idx_socio_cpf_nome), and expansion stops once one edge more than the requested max_edges has been found, so hubs
stay as cheap as ordinary companies. Only the latest reference month is used.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from core.database import DatabaseManager

EDGE_COLUMNS = """
    s.id, s.cnpj_basico, s.identificador_socio, s.cpf_cnpj_socio, s.nome_socio_razao_social,
    s.faixa_etaria, s.qualificacao_socio, s.data_entrada_sociedade, s.reference_month
"""

# Rows that cannot become an edge: a corporate partner without a full CNPJ, or a person with neither
# CPF nor name. Filtered in SQL so that they do not use up the LIMIT.
VALID_PARTNER = """
    CASE WHEN s.identificador_socio = 1 THEN length(s.cpf_cnpj_socio) >= 14
         ELSE coalesce(nullif(s.cpf_cnpj_socio, ''), nullif(s.nome_socio_razao_social, '')) IS NOT NULL END
"""

PARTNERS_OF_COMPANIES = text(f"""
    SELECT {EDGE_COLUMNS} FROM cnpj.socio s
    WHERE s.cnpj_basico = ANY(:companies) AND s.reference_month = :month AND s.id <> ALL(:seen)
      AND {VALID_PARTNER}
    LIMIT :limit
""")

COMPANIES_AS_PARTNERS = text(f"""
    SELECT {EDGE_COLUMNS} FROM cnpj.socio s
    WHERE s.identificador_socio = 1 AND left(s.cpf_cnpj_socio, 8) = ANY(:companies)
      AND s.reference_month = :month AND s.id <> ALL(:seen)
      AND {VALID_PARTNER}
    LIMIT :limit
""")

COMPANIES_OF_PEOPLE = text(f"""
    SELECT {EDGE_COLUMNS} FROM cnpj.socio s
    JOIN unnest(CAST(:cpfs AS text[]), CAST(:names AS text[])) AS p(cpf, nome)
      ON s.cpf_cnpj_socio = p.cpf AND s.nome_socio_razao_social = p.nome
    WHERE s.identificador_socio IN (2, 3) AND s.reference_month = :month AND s.id <> ALL(:seen)
      AND {VALID_PARTNER}
    LIMIT :limit
""")


def _partner_key(row: dict[str, Any]) -> tuple[str, Any]:
    """The node a socio row starts from: a company (corporate partner) or a person."""
    document = row["cpf_cnpj_socio"] or ""
    if row["identificador_socio"] == 1:
        return ("empresa", document[:8])
    return ("pessoa", (document, row["nome_socio_razao_social"] or ""))


async def _expand(
    connection: AsyncConnection,
    start: tuple[str, Any],
    hops: int,
    max_edges: int,
) -> tuple[list[tuple[dict[str, Any], tuple[str, Any]]], bool]:
    """Edges (socio row, partner node) within `hops` of `start`, nearest first, and whether truncated."""
    month = (await connection.execute(text("SELECT max(reference_month) FROM cnpj.socio"))).scalar()
    edges: list[tuple[dict[str, Any], tuple[str, Any]]] = []
    seen_edges: list[int] = []
    seen_nodes = {start}
    frontier = [start]

    for _ in range(hops):
        companies = [key for kind, key in frontier if kind == "empresa"]
        people = [key for kind, key in frontier if kind == "pessoa"]
        lookups = []
        if companies:
            lookups += [(PARTNERS_OF_COMPANIES, {"companies": companies}), (COMPANIES_AS_PARTNERS, {"companies": companies})]
        if people:
            lookups.append((COMPANIES_OF_PEOPLE, {"cpfs": [c for c, _ in people], "names": [n for _, n in people]}))

        next_frontier: list[tuple[str, Any]] = []
        for query, params in lookups:
            limit = max_edges + 1 - len(edges)
            result = await connection.execute(query, {**params, "month": month, "seen": seen_edges, "limit": limit})
            for row in result.mappings():
                seen_edges.append(row["id"])
                partner = _partner_key(row)
                edges.append((dict(row), partner))
                for node in (partner, ("empresa", row["cnpj_basico"])):
                    if node not in seen_nodes:
                        seen_nodes.add(node)
                        next_frontier.append(node)
            if len(edges) > max_edges:
                return edges[:max_edges], True
        frontier = next_frontier
        if not frontier:
            break
    return edges, False


async def _companies(connection: AsyncConnection, cnpjs: set[str]) -> dict[str, dict[str, Any]]:
    result = await connection.execute(
        text(
            "SELECT cnpj_basico, razao_social, porte_empresa, natureza_juridica "
            "FROM cnpj.empresa WHERE cnpj_basico = ANY(:cnpjs)"
        ),
        {"cnpjs": list(cnpjs)},
    )
    return {row["cnpj_basico"]: dict(row) for row in result.mappings()}


def _edge(row: dict[str, Any], source_id: str) -> dict[str, Any]:
    entry = row["data_entrada_sociedade"]
    return {
        "id": f"edge:{row['id']}",
        "source": source_id,
        "target": f"empresa:{row['cnpj_basico']}",
        "type": "SOCIO_DE",
        "data": {
            "qualificacao_socio": row["qualificacao_socio"],
            "data_entrada_sociedade": entry.isoformat() if entry else None,
            "reference_month": row["reference_month"],
        },
    }


def _company_node(cnpj: str, company: dict[str, Any] | None, fallback_label: str | None, is_core: bool) -> dict[str, Any]:
    company = company or {}
    label = company.get("razao_social") or fallback_label or cnpj
    return {
        "id": f"empresa:{cnpj}",
        "type": "empresa",
        "label": label,
        "data": {
            "cnpj_basico": cnpj,
            "razao_social": company.get("razao_social") or fallback_label,
            "porte_empresa": company.get("porte_empresa"),
            "natureza_juridica": company.get("natureza_juridica"),
            "is_core": is_core,
        },
    }


def _metadata(core: dict[str, Any], depth: int, nodes: list, edges: list, truncated: bool, max_edges: int) -> dict:
    return {
        **core,
        "depth": depth,
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "total_relationships": len(edges) + (1 if truncated else 0),
        "truncated": truncated,
        "max_edges": max_edges,
    }


class CnpjNetworkService:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    def _limits(self, depth: int, max_edges: int) -> tuple[int, int]:
        settings = self.database.settings
        return (
            max(1, min(depth, max(1, settings.network_max_depth))),
            max(1, min(max_edges, max(1, settings.network_max_edges))),
        )

    async def get_company_network(self, cnpj_basico: str, depth: int = 1, max_edges: int = 200) -> dict[str, Any] | None:
        depth, max_edges = self._limits(depth, max_edges)
        try:
            async with self.database.postgres_connection() as connection:
                if not await _companies(connection, {cnpj_basico}):
                    return None
                # Depth counts company-to-company steps: depth 1 is the direct partners, depth 2 adds
                # the partners' other companies and their partners, and so on.
                edges, truncated = await _expand(connection, ("empresa", cnpj_basico), depth * 2 - 1, max_edges)
                cnpjs = {cnpj_basico} | {row["cnpj_basico"] for row, _ in edges}
                cnpjs |= {key for (_, (kind, key)) in edges if kind == "empresa"}
                companies = await _companies(connection, cnpjs)
        except SQLAlchemyError as exc:
            raise RuntimeError("network_query_failed") from exc

        nodes = {f"empresa:{cnpj_basico}": _company_node(cnpj_basico, companies.get(cnpj_basico), None, True)}
        edge_list = []
        for row, (kind, key) in edges:
            if kind == "empresa":
                source_id = f"empresa:{key}"
                nodes.setdefault(source_id, _company_node(key, companies.get(key), row["nome_socio_razao_social"], False))
            else:
                cpf, name = key
                source_id = f"socio:{cpf}:{name}"
                nodes.setdefault(source_id, {
                    "id": source_id,
                    "type": "socio",
                    "label": name or cpf or "Sócio",
                    "data": {
                        "cnpj_basico": None,
                        "razao_social": None,
                        "porte_empresa": None,
                        "natureza_juridica": None,
                        "cpf_cnpj_socio": cpf or None,
                        "identificador_socio": row["identificador_socio"],
                        "faixa_etaria": row["faixa_etaria"],
                        "is_core": False,
                    },
                })
            target = row["cnpj_basico"]
            nodes.setdefault(f"empresa:{target}", _company_node(target, companies.get(target), None, False))
            edge_list.append(_edge(row, source_id))

        node_list = list(nodes.values())
        return {
            "nodes": node_list,
            "edges": edge_list,
            "metadata": _metadata({"core_cnpj_basico": cnpj_basico}, depth, node_list, edge_list, truncated, max_edges),
        }

    async def get_person_network(
        self, cpf_masked: str, nome: str, depth: int = 1, max_edges: int = 200
    ) -> dict[str, Any] | None:
        depth, max_edges = self._limits(depth, max_edges)
        try:
            async with self.database.postgres_connection() as connection:
                person = (await connection.execute(
                    text(
                        "SELECT identificador_socio, faixa_etaria FROM cnpj.socio "
                        "WHERE cpf_cnpj_socio = :cpf AND nome_socio_razao_social = :nome "
                        "AND identificador_socio IN (2, 3) ORDER BY reference_month DESC LIMIT 1"
                    ),
                    {"cpf": cpf_masked, "nome": nome},
                )).mappings().first()
                if person is None:
                    return None
                edges, truncated = await _expand(connection, ("pessoa", (cpf_masked, nome)), depth, max_edges)
                cnpjs = {row["cnpj_basico"] for row, _ in edges}
                cnpjs |= {key for (_, (kind, key)) in edges if kind == "empresa"}
                companies = await _companies(connection, cnpjs)
        except SQLAlchemyError as exc:
            raise RuntimeError("network_query_failed") from exc

        core_id = f"pessoa:{cpf_masked}:{nome}"
        nodes = {
            core_id: {
                "id": core_id,
                "type": "pessoa",
                "label": nome or cpf_masked,
                "data": {
                    "cpf_cnpj_socio": cpf_masked,
                    "nome": nome or None,
                    "identificador_socio": person["identificador_socio"],
                    "faixa_etaria": person["faixa_etaria"],
                    "is_core": True,
                },
            }
        }
        edge_list = []
        for row, (kind, key) in edges:
            if kind == "empresa":
                source_id = f"empresa:{key}"
                nodes.setdefault(source_id, _company_node(key, companies.get(key), row["nome_socio_razao_social"], False))
            else:
                cpf, name = key
                source_id = f"pessoa:{cpf}:{name}"
                nodes.setdefault(source_id, {
                    "id": source_id,
                    "type": "pessoa",
                    "label": name or cpf or "Pessoa",
                    "data": {
                        "cpf_cnpj_socio": cpf or None,
                        "nome": name or None,
                        "identificador_socio": row["identificador_socio"],
                        "faixa_etaria": row["faixa_etaria"],
                        "is_core": False,
                    },
                })
            target = row["cnpj_basico"]
            nodes.setdefault(f"empresa:{target}", _company_node(target, companies.get(target), None, False))
            edge_list.append(_edge(row, source_id))

        node_list = list(nodes.values())
        return {
            "nodes": node_list,
            "edges": edge_list,
            "metadata": _metadata(
                {"core_cpf_mascarado": cpf_masked, "core_nome": nome or None},
                depth, node_list, edge_list, truncated, max_edges,
            ),
        }
