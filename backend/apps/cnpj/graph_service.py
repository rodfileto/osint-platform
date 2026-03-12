from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from django.conf import settings


class CompanyNetworkService:
    def __init__(self):
        self._driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )

    def get_company_network(self, cnpj_basico: str):
        max_edges = max(1, settings.NEO4J_MAX_NETWORK_EDGES)

        company_query = """
        MATCH (e:Empresa {cnpj_basico: $cnpj_basico})
        RETURN e
        LIMIT 1
        """

        total_relationships_query = """
        MATCH (e:Empresa {cnpj_basico: $cnpj_basico})
        OPTIONAL MATCH (:Pessoa)-[r:SOCIO_DE]->(e)
        RETURN count(r) AS total_relationships
        """

        network_query = """
        MATCH (e:Empresa {cnpj_basico: $cnpj_basico})
        OPTIONAL MATCH (s:Pessoa)-[r:SOCIO_DE]->(e)
        WITH e, s, r
        ORDER BY coalesce(s.nome, s.nome_socio_razao_social, s.cpf_cnpj_socio, '')
        LIMIT $max_edges
        RETURN e,
               collect(DISTINCT s) AS socios,
               collect(
                   CASE
                       WHEN r IS NULL THEN NULL
                       ELSE {
                           id: elementId(r),
                           source_identifier: coalesce(s.cpf_cnpj_socio, s.cpf_cnpj, s.documento, elementId(s)),
                           target_identifier: coalesce(e.cnpj_basico, elementId(e)),
                           type: type(r),
                           qualificacao_socio: r.qualificacao_socio,
                           data_entrada_sociedade: toString(r.data_entrada_sociedade),
                           reference_month: r.reference_month
                       }
                   END
               ) AS rels
        """

        try:
            with self._driver.session() as session:
                company_record = session.run(
                    company_query,
                    cnpj_basico=cnpj_basico,
                ).single()

                if not company_record:
                    return None

                total_record = session.run(
                    total_relationships_query,
                    cnpj_basico=cnpj_basico,
                ).single()

                network_record = session.run(
                    network_query,
                    cnpj_basico=cnpj_basico,
                    max_edges=max_edges,
                ).single()
        except Neo4jError as exc:
            raise RuntimeError("neo4j_query_failed") from exc

        empresa = company_record["e"]
        socios = [node for node in ((network_record or {}).get("socios") or []) if node is not None]
        relationships = [
            rel for rel in (((network_record or {}).get("rels") or []))
            if rel and rel.get("id") is not None
        ]
        total_relationships = int(((total_record or {}).get("total_relationships")) or 0)

        empresa_cnpj = str(empresa.get("cnpj_basico") or cnpj_basico)
        empresa_node_id = f"empresa:{empresa_cnpj}"

        nodes = [
            {
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
        ]

        edges = []
        for socio in socios:
            socio_identifier = (
                socio.get("cpf_cnpj_socio")
                or socio.get("cpf_cnpj")
                or socio.get("documento")
                or socio.element_id
            )
            socio_id = f"socio:{socio_identifier}"
            nodes.append(
                {
                    "id": socio_id,
                    "type": "socio",
                    "label": socio.get("nome") or socio.get("nome_socio_razao_social") or "Sócio",
                    "data": {
                        "cpf_cnpj_socio": socio.get("cpf_cnpj_socio") or socio.get("cpf_cnpj"),
                        "identificador_socio": socio.get("identificador_socio"),
                        "faixa_etaria": socio.get("faixa_etaria"),
                    },
                }
            )

        for rel in relationships:
            source_id = f"socio:{rel.get('source_identifier')}"
            target_id = f"empresa:{rel.get('target_identifier')}"

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

        nodes_by_id = {node["id"]: node for node in nodes}
        unique_nodes = list(nodes_by_id.values())

        return {
            "nodes": unique_nodes,
            "edges": edges,
            "metadata": {
                "core_cnpj_basico": empresa_cnpj,
                "depth": 1,
                "total_nodes": len(unique_nodes),
                "total_edges": len(edges),
                "total_relationships": total_relationships,
                "truncated": total_relationships > len(edges),
                "max_edges": max_edges,
            },
        }
