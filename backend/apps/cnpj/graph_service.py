from typing import LiteralString, cast

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from django.conf import settings


class CompanyNetworkService:
    def __init__(self, driver=None):
        self._driver = driver or GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )

    def get_company_network(self, cnpj_basico: str, depth: int = 1):
        max_depth = max(1, settings.NEO4J_MAX_NETWORK_DEPTH)
        normalized_depth = max(1, min(depth, max_depth))
        max_hops = (normalized_depth * 2) - 1
        max_edges = max(1, settings.NEO4J_MAX_NETWORK_EDGES)
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
            source_capital_social: startNode(rel).capital_social,
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
            target_capital_social: endNode(rel).capital_social,
            type: type(rel),
            qualificacao_socio: rel.qualificacao_socio,
            data_entrada_sociedade: toString(rel.data_entrada_sociedade),
            reference_month: rel.reference_month
        }}) AS rels
        """

        try:
            with self._driver.session() as session:
                company_record = session.run(
                    company_query,
                    cnpj_basico=cnpj_basico,
                ).single()

                if not company_record:
                    return None

                network_record = session.run(
                    cast(LiteralString, network_query),
                    cnpj_basico=cnpj_basico,
                    relationship_limit=relationship_limit,
                ).single()
        except Neo4jError as exc:
            raise RuntimeError("neo4j_query_failed") from exc

        empresa = company_record["e"]
        relationship_rows = [
            rel for rel in (((network_record or {}).get("rels") or []))
            if rel and rel.get("id") is not None
        ]
        truncated = len(relationship_rows) > max_edges
        relationships = relationship_rows[:max_edges]
        total_relationships = len(relationships) + (1 if truncated else 0)

        empresa_cnpj = str(empresa.get("cnpj_basico") or cnpj_basico)
        empresa_node_id = f"empresa:{empresa_cnpj}"

        nodes_by_id = {
            empresa_node_id: {
                "id": empresa_node_id,
                "type": "empresa",
                "label": empresa.get("razao_social") or empresa.get("nome") or empresa_cnpj,
                "data": {
                    "cnpj_basico": empresa_cnpj,
                    "razao_social": empresa.get("razao_social"),
                    "porte_empresa": empresa.get("porte_empresa"),
                    "natureza_juridica": empresa.get("natureza_juridica"),
                    "capital_social": empresa.get("capital_social"),
                    "is_core": True,
                },
            }
        }

        edges = []
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
                        "capital_social": rel.get("source_capital_social") if source_kind == "empresa" else None,
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
                        "capital_social": rel.get("target_capital_social"),
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
