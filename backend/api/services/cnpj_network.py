"""Async Neo4j-backed CNPJ network service."""

from __future__ import annotations

from typing import Any

from neo4j.exceptions import Neo4jError

from core.database import DatabaseManager


class CnpjNetworkService:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def get_company_network(self, cnpj_basico: str, depth: int = 1) -> dict[str, Any] | None:
        settings = self.database.settings
        normalized_depth = max(1, min(depth, max(1, settings.neo4j_max_network_depth)))
        max_hops = (normalized_depth * 2) - 1
        max_edges = max(1, settings.neo4j_max_network_edges)
        relationship_limit = max_edges + 1

        company_query = """
        MATCH (e:Empresa {cnpj_basico: $cnpj_basico})
        RETURN e
        LIMIT 1
        """

        network_query = f"""
        MATCH (core:Empresa {{cnpj_basico: $cnpj_basico}})
        MATCH path = (core)-[:SOCIO_DE*1..{max_hops}]-(connected)
        UNWIND relationships(path) AS rel
        WITH DISTINCT rel
        ORDER BY elementId(rel)
        LIMIT $relationship_limit
        RETURN collect({{
            id: elementId(rel),
            source_kind: CASE
                WHEN startNode(rel):Empresa THEN 'empresa'
                ELSE 'socio'
            END,
            source_identifier: coalesce(
                startNode(rel).cnpj_basico,
                startNode(rel).cpf_cnpj_socio,
                startNode(rel).cpf_cnpj,
                startNode(rel).documento,
                elementId(startNode(rel))
            ),
            source_label: coalesce(
                startNode(rel).razao_social,
                startNode(rel).nome,
                startNode(rel).nome_socio_razao_social,
                startNode(rel).cnpj_basico,
                startNode(rel).cpf_cnpj_socio,
                'Sócio'
            ),
            source_cnpj_basico: startNode(rel).cnpj_basico,
            source_cpf_cnpj_socio: coalesce(startNode(rel).cpf_cnpj_socio, startNode(rel).cpf_cnpj),
            source_identificador_socio: startNode(rel).identificador_socio,
            source_faixa_etaria: startNode(rel).faixa_etaria,
            source_porte_empresa: startNode(rel).porte_empresa,
            source_natureza_juridica: startNode(rel).natureza_juridica,
            target_identifier: coalesce(endNode(rel).cnpj_basico, elementId(endNode(rel))),
            target_label: coalesce(
                endNode(rel).razao_social,
                endNode(rel).nome,
                endNode(rel).nome_socio_razao_social,
                endNode(rel).cnpj_basico,
                'Empresa'
            ),
            target_cnpj_basico: endNode(rel).cnpj_basico,
            target_porte_empresa: endNode(rel).porte_empresa,
            target_natureza_juridica: endNode(rel).natureza_juridica,
            type: type(rel),
            qualificacao_socio: rel.qualificacao_socio,
            data_entrada_sociedade: toString(rel.data_entrada_sociedade),
            reference_month: rel.reference_month
        }}) AS rels
        """

        try:
            async with self.database.neo4j_session() as session:
                company_result = await session.run(company_query, cnpj_basico=cnpj_basico)
                company_record = await company_result.single()
                if not company_record:
                    return None

                network_result = await session.run(
                    network_query,
                    cnpj_basico=cnpj_basico,
                    relationship_limit=relationship_limit,
                )
                network_record = await network_result.single()
        except Neo4jError as exc:
            raise RuntimeError("neo4j_query_failed") from exc

        empresa = company_record["e"]
        relationship_rows = [
            rel for rel in (((network_record or {}).get("rels") or [])) if rel and rel.get("id") is not None
        ]
        truncated = len(relationship_rows) > max_edges
        relationships = relationship_rows[:max_edges]
        total_relationships = len(relationships) + (1 if truncated else 0)

        empresa_cnpj = str(empresa.get("cnpj_basico") or cnpj_basico)
        empresa_node_id = f"empresa:{empresa_cnpj}"
        nodes_by_id: dict[str, dict[str, Any]] = {
            empresa_node_id: {
                "id": empresa_node_id,
                "type": "empresa",
                "label": empresa.get("razao_social") or empresa.get("nome") or empresa_cnpj,
                "data": {
                    "cnpj_basico": empresa_cnpj,
                    "razao_social": empresa.get("razao_social"),
                    "porte_empresa": empresa.get("porte_empresa"),
                    "natureza_juridica": empresa.get("natureza_juridica"),
                    "is_core": True,
                },
            }
        }

        edges: list[dict[str, Any]] = []
        for rel in relationships:
            source_kind = rel.get("source_kind") or "socio"
            source_id = f"{source_kind}:{rel.get('source_identifier')}"
            target_id = f"empresa:{rel.get('target_identifier')}"

            if source_id not in nodes_by_id:
                nodes_by_id[source_id] = {
                    "id": source_id,
                    "type": source_kind,
                    "label": rel.get("source_label") or "Sócio",
                    "data": {
                        "cnpj_basico": rel.get("source_cnpj_basico") if source_kind == "empresa" else None,
                        "razao_social": rel.get("source_label") if source_kind == "empresa" else None,
                        "porte_empresa": rel.get("source_porte_empresa") if source_kind == "empresa" else None,
                        "natureza_juridica": rel.get("source_natureza_juridica") if source_kind == "empresa" else None,
                        "cpf_cnpj_socio": None if source_kind == "empresa" else rel.get("source_cpf_cnpj_socio"),
                        "identificador_socio": rel.get("source_identificador_socio"),
                        "faixa_etaria": rel.get("source_faixa_etaria"),
                        "is_core": False,
                    },
                }

            if target_id not in nodes_by_id:
                nodes_by_id[target_id] = {
                    "id": target_id,
                    "type": "empresa",
                    "label": rel.get("target_label") or rel.get("target_identifier") or "Empresa",
                    "data": {
                        "cnpj_basico": rel.get("target_cnpj_basico"),
                        "razao_social": rel.get("target_label"),
                        "porte_empresa": rel.get("target_porte_empresa"),
                        "natureza_juridica": rel.get("target_natureza_juridica"),
                        "is_core": target_id == empresa_node_id,
                    },
                }

            edges.append(
                {
                    "id": f"edge:{rel.get('id')}",
                    "source": source_id,
                    "target": target_id,
                    "type": rel.get("type") or "SOCIO_DE",
                    "data": {
                        "qualificacao_socio": rel.get("qualificacao_socio"),
                        "data_entrada_sociedade": rel.get("data_entrada_sociedade"),
                        "reference_month": rel.get("reference_month"),
                    },
                }
            )

        unique_nodes = list(nodes_by_id.values())
        return {
            "nodes": unique_nodes,
            "edges": edges,
            "metadata": {
                "core_cnpj_basico": empresa_cnpj,
                "depth": normalized_depth,
                "total_nodes": len(unique_nodes),
                "total_edges": len(edges),
                "total_relationships": total_relationships,
                "truncated": truncated,
                "max_edges": max_edges,
            },
        }

    async def get_person_network(
        self,
        cpf_masked: str,
        nome: str | None = None,
        depth: int = 1,
    ) -> dict[str, Any] | None:
        settings = self.database.settings
        normalized_depth = max(1, min(depth, max(1, settings.neo4j_max_network_depth)))
        max_hops = normalized_depth
        max_edges = max(1, settings.neo4j_max_network_edges)
        relationship_limit = max_edges + 1

        person_query = """
        MATCH (p:Pessoa)
        WHERE p.cpf_cnpj_socio = $cpf_masked
          AND ($nome IS NULL OR p.nome = $nome)
        RETURN p
        ORDER BY p.nome
        LIMIT 1
        """

        network_query = f"""
        MATCH (core:Pessoa)
        WHERE core.cpf_cnpj_socio = $cpf_masked
          AND ($nome IS NULL OR core.nome = $nome)
        WITH core ORDER BY core.nome LIMIT 1
        MATCH path = (core)-[:SOCIO_DE*1..{max_hops}]-(connected)
        UNWIND relationships(path) AS rel
        WITH DISTINCT rel
        ORDER BY elementId(rel)
        LIMIT $relationship_limit
        RETURN collect({{
            id: elementId(rel),
            source_kind: CASE
                WHEN startNode(rel):Empresa THEN 'empresa'
                ELSE 'pessoa'
            END,
            source_identifier: coalesce(
                startNode(rel).cnpj_basico,
                startNode(rel).cpf_cnpj_socio,
                elementId(startNode(rel))
            ),
            source_label: coalesce(
                startNode(rel).razao_social,
                startNode(rel).nome,
                startNode(rel).cnpj_basico,
                'Desconhecido'
            ),
            source_cnpj_basico: startNode(rel).cnpj_basico,
            source_cpf_cnpj_socio: startNode(rel).cpf_cnpj_socio,
            source_nome: startNode(rel).nome,
            source_identificador_socio: startNode(rel).identificador_socio,
            source_faixa_etaria: startNode(rel).faixa_etaria,
            source_porte_empresa: startNode(rel).porte_empresa,
            source_natureza_juridica: startNode(rel).natureza_juridica,
            target_kind: CASE
                WHEN endNode(rel):Empresa THEN 'empresa'
                ELSE 'pessoa'
            END,
            target_identifier: coalesce(
                endNode(rel).cnpj_basico,
                endNode(rel).cpf_cnpj_socio,
                elementId(endNode(rel))
            ),
            target_label: coalesce(
                endNode(rel).razao_social,
                endNode(rel).nome,
                endNode(rel).cnpj_basico,
                'Desconhecido'
            ),
            target_cnpj_basico: endNode(rel).cnpj_basico,
            target_cpf_cnpj_socio: endNode(rel).cpf_cnpj_socio,
            target_porte_empresa: endNode(rel).porte_empresa,
            target_natureza_juridica: endNode(rel).natureza_juridica,
            type: type(rel),
            qualificacao_socio: rel.qualificacao_socio,
            data_entrada_sociedade: toString(rel.data_entrada_sociedade),
            reference_month: rel.reference_month
        }}) AS rels
        """

        try:
            async with self.database.neo4j_session() as session:
                person_result = await session.run(person_query, cpf_masked=cpf_masked, nome=nome)
                person_record = await person_result.single()
                if not person_record:
                    return None

                network_result = await session.run(
                    network_query,
                    cpf_masked=cpf_masked,
                    nome=nome,
                    relationship_limit=relationship_limit,
                )
                network_record = await network_result.single()
        except Neo4jError as exc:
            raise RuntimeError("neo4j_query_failed") from exc

        pessoa = person_record["p"]
        relationship_rows = [
            rel for rel in (((network_record or {}).get("rels") or [])) if rel and rel.get("id") is not None
        ]
        truncated = len(relationship_rows) > max_edges
        relationships = relationship_rows[:max_edges]
        total_relationships = len(relationships) + (1 if truncated else 0)

        pessoa_cpf = str(pessoa.get("cpf_cnpj_socio") or cpf_masked)
        pessoa_nome = str(pessoa.get("nome") or nome or "")
        core_node_id = f"pessoa:{pessoa_cpf}:{pessoa_nome}"
        nodes_by_id: dict[str, dict[str, Any]] = {
            core_node_id: {
                "id": core_node_id,
                "type": "pessoa",
                "label": pessoa_nome or pessoa_cpf,
                "data": {
                    "cpf_cnpj_socio": pessoa_cpf,
                    "nome": pessoa_nome or None,
                    "identificador_socio": pessoa.get("identificador_socio"),
                    "faixa_etaria": pessoa.get("faixa_etaria"),
                    "is_core": True,
                },
            }
        }

        edges: list[dict[str, Any]] = []
        for rel in relationships:
            source_kind = rel.get("source_kind") or "pessoa"
            source_cpf = rel.get("source_cpf_cnpj_socio") or rel.get("source_identifier")
            source_nome_val = rel.get("source_nome") or rel.get("source_label") or ""
            source_id = (
                f"empresa:{rel.get('source_identifier')}"
                if source_kind == "empresa"
                else f"pessoa:{source_cpf}:{source_nome_val}"
            )

            target_kind = rel.get("target_kind") or "empresa"
            target_cpf = rel.get("target_cpf_cnpj_socio") or rel.get("target_identifier")
            target_id = (
                f"empresa:{rel.get('target_identifier')}"
                if target_kind == "empresa"
                else f"pessoa:{target_cpf}:{rel.get('target_label') or ''}"
            )

            if source_id not in nodes_by_id:
                if source_kind == "empresa":
                    nodes_by_id[source_id] = {
                        "id": source_id,
                        "type": "empresa",
                        "label": rel.get("source_label") or "Empresa",
                        "data": {
                            "cnpj_basico": rel.get("source_cnpj_basico"),
                            "razao_social": rel.get("source_label"),
                            "porte_empresa": rel.get("source_porte_empresa"),
                            "natureza_juridica": rel.get("source_natureza_juridica"),
                            "is_core": False,
                        },
                    }
                else:
                    nodes_by_id[source_id] = {
                        "id": source_id,
                        "type": "pessoa",
                        "label": rel.get("source_label") or "Pessoa",
                        "data": {
                            "cpf_cnpj_socio": source_cpf,
                            "nome": rel.get("source_nome") or rel.get("source_label"),
                            "identificador_socio": rel.get("source_identificador_socio"),
                            "faixa_etaria": rel.get("source_faixa_etaria"),
                            "is_core": source_id == core_node_id,
                        },
                    }

            if target_id not in nodes_by_id:
                if target_kind == "empresa":
                    nodes_by_id[target_id] = {
                        "id": target_id,
                        "type": "empresa",
                        "label": rel.get("target_label") or rel.get("target_identifier") or "Empresa",
                        "data": {
                            "cnpj_basico": rel.get("target_cnpj_basico"),
                            "razao_social": rel.get("target_label"),
                            "porte_empresa": rel.get("target_porte_empresa"),
                            "natureza_juridica": rel.get("target_natureza_juridica"),
                            "is_core": False,
                        },
                    }
                else:
                    nodes_by_id[target_id] = {
                        "id": target_id,
                        "type": "pessoa",
                        "label": rel.get("target_label") or "Pessoa",
                        "data": {
                            "cpf_cnpj_socio": target_cpf,
                            "nome": rel.get("target_label"),
                            "is_core": False,
                        },
                    }

            edges.append(
                {
                    "id": f"edge:{rel.get('id')}",
                    "source": source_id,
                    "target": target_id,
                    "type": rel.get("type") or "SOCIO_DE",
                    "data": {
                        "qualificacao_socio": rel.get("qualificacao_socio"),
                        "data_entrada_sociedade": rel.get("data_entrada_sociedade"),
                        "reference_month": rel.get("reference_month"),
                    },
                }
            )

        unique_nodes = list(nodes_by_id.values())
        return {
            "nodes": unique_nodes,
            "edges": edges,
            "metadata": {
                "core_cpf_mascarado": pessoa_cpf,
                "core_nome": pessoa_nome or None,
                "depth": normalized_depth,
                "total_nodes": len(unique_nodes),
                "total_edges": len(edges),
                "total_relationships": total_relationships,
                "truncated": truncated,
                "max_edges": max_edges,
            },
        }
