from unittest.mock import MagicMock, Mock

from django.test import SimpleTestCase, override_settings

from .graph_service import CompanyNetworkService


class FakeNode(dict):
	def __init__(self, properties, *, labels, element_id):
		super().__init__(properties)
		self.labels = set(labels)
		self.element_id = element_id


def _single_result(record):
	result = Mock()
	result.single.return_value = record
	return result


@override_settings(NEO4J_MAX_NETWORK_EDGES=50, NEO4J_MAX_NETWORK_DEPTH=3)
class CompanyNetworkServiceTests(SimpleTestCase):
	def test_get_company_network_includes_pessoa_juridica_shareholders(self):
		company_node = FakeNode(
			{
				"cnpj_basico": "12345678",
				"razao_social": "Empresa Alvo SA",
				"porte_empresa": "05",
				"natureza_juridica": "2054",
			},
			labels={"Empresa"},
			element_id="empresa-alvo",
		)

		driver = Mock()
		session = Mock()
		session_context = MagicMock()
		session_context.__enter__.return_value = session
		session_context.__exit__.return_value = None
		driver.session.return_value = session_context
		session.run.side_effect = [
			_single_result({"e": company_node}),
			_single_result(
				{
					"rels": [
						{
							"id": "rel-pf",
							"source_identifier": "***123456**",
							"source_kind": "socio",
							"source_label": "JOAO DA SILVA",
							"source_cnpj_basico": None,
							"source_cpf_cnpj_socio": "***123456**",
							"source_identificador_socio": 2,
							"source_faixa_etaria": "6",
							"source_porte_empresa": None,
							"source_natureza_juridica": None,
							"target_identifier": "12345678",
							"target_label": "Empresa Alvo SA",
							"target_cnpj_basico": "12345678",
							"target_porte_empresa": "05",
							"target_natureza_juridica": "2054",
							"type": "SOCIO_DE",
							"qualificacao_socio": "49",
							"data_entrada_sociedade": "2024-01-01",
							"reference_month": "2026-02",
						},
						{
							"id": "rel-pj",
							"source_identifier": "87654321",
							"source_kind": "empresa",
							"source_label": "Holding XPTO LTDA",
							"source_cnpj_basico": "87654321",
							"source_cpf_cnpj_socio": None,
							"source_identificador_socio": None,
							"source_faixa_etaria": None,
							"source_porte_empresa": "03",
							"source_natureza_juridica": "2062",
							"target_identifier": "12345678",
							"target_label": "Empresa Alvo SA",
							"target_cnpj_basico": "12345678",
							"target_porte_empresa": "05",
							"target_natureza_juridica": "2054",
							"type": "SOCIO_DE",
							"qualificacao_socio": "22",
							"data_entrada_sociedade": "2023-06-10",
							"reference_month": "2026-02",
						},
					]
				}
			),
		]

		service = CompanyNetworkService(driver=driver)

		result = service.get_company_network("12345678")

		self.assertIsNotNone(result)
		assert result is not None

		nodes_by_id = {node["id"]: node for node in result["nodes"]}
		self.assertIn("empresa:12345678", nodes_by_id)
		self.assertIn("socio:***123456**", nodes_by_id)
		self.assertIn("empresa:87654321", nodes_by_id)
		self.assertEqual(nodes_by_id["empresa:87654321"]["type"], "empresa")
		self.assertEqual(nodes_by_id["empresa:87654321"]["label"], "Holding XPTO LTDA")
		self.assertEqual(nodes_by_id["empresa:87654321"]["data"]["cnpj_basico"], "87654321")
		self.assertNotIn("capital_social", nodes_by_id["empresa:12345678"]["data"])
		self.assertNotIn("capital_social", nodes_by_id["empresa:87654321"]["data"])

		edges_by_id = {edge["id"]: edge for edge in result["edges"]}
		self.assertEqual(edges_by_id["edge:rel-pf"]["source"], "socio:***123456**")
		self.assertEqual(edges_by_id["edge:rel-pj"]["source"], "empresa:87654321")
		self.assertEqual(result["metadata"]["depth"], 1)
		self.assertEqual(result["metadata"]["total_relationships"], 2)
		self.assertFalse(result["metadata"]["truncated"])

	def test_get_company_network_depth_two_includes_related_companies_and_partners(self):
		company_node = FakeNode(
			{
				"cnpj_basico": "12345678",
				"razao_social": "Empresa Alvo SA",
				"porte_empresa": "05",
				"natureza_juridica": "2054",
			},
			labels={"Empresa"},
			element_id="empresa-alvo",
		)

		driver = Mock()
		session = Mock()
		session_context = MagicMock()
		session_context.__enter__.return_value = session
		session_context.__exit__.return_value = None
		driver.session.return_value = session_context
		session.run.side_effect = [
			_single_result({"e": company_node}),
			_single_result(
				{
					"rels": [
						{
							"id": "rel-1",
							"source_identifier": "87654321",
							"source_kind": "empresa",
							"source_label": "Holding XPTO LTDA",
							"source_cnpj_basico": "87654321",
							"source_cpf_cnpj_socio": None,
							"source_identificador_socio": None,
							"source_faixa_etaria": None,
							"source_porte_empresa": "03",
							"source_natureza_juridica": "2062",
							"target_identifier": "12345678",
							"target_label": "Empresa Alvo SA",
							"target_cnpj_basico": "12345678",
							"target_porte_empresa": "05",
							"target_natureza_juridica": "2054",
							"type": "SOCIO_DE",
							"qualificacao_socio": "22",
							"data_entrada_sociedade": "2023-06-10",
							"reference_month": "2026-02",
						},
						{
							"id": "rel-2",
							"source_identifier": "87654321",
							"source_kind": "empresa",
							"source_label": "Holding XPTO LTDA",
							"source_cnpj_basico": "87654321",
							"source_cpf_cnpj_socio": None,
							"source_identificador_socio": None,
							"source_faixa_etaria": None,
							"source_porte_empresa": "03",
							"source_natureza_juridica": "2062",
							"target_identifier": "22223333",
							"target_label": "Empresa Investida Ltda",
							"target_cnpj_basico": "22223333",
							"target_porte_empresa": "01",
							"target_natureza_juridica": "2062",
							"type": "SOCIO_DE",
							"qualificacao_socio": "22",
							"data_entrada_sociedade": "2022-05-01",
							"reference_month": "2026-02",
						},
						{
							"id": "rel-3",
							"source_identifier": "***999000**",
							"source_kind": "socio",
							"source_label": "MARIA DE SOUZA",
							"source_cnpj_basico": None,
							"source_cpf_cnpj_socio": "***999000**",
							"source_identificador_socio": 2,
							"source_faixa_etaria": "7",
							"source_porte_empresa": None,
							"source_natureza_juridica": None,
							"target_identifier": "22223333",
							"target_label": "Empresa Investida Ltda",
							"target_cnpj_basico": "22223333",
							"target_porte_empresa": "01",
							"target_natureza_juridica": "2062",
							"type": "SOCIO_DE",
							"qualificacao_socio": "49",
							"data_entrada_sociedade": "2020-03-15",
							"reference_month": "2026-02",
						},
					]
				}
			),
		]

		service = CompanyNetworkService(driver=driver)

		result = service.get_company_network("12345678", depth=2)

		self.assertIsNotNone(result)
		assert result is not None

		nodes_by_id = {node["id"]: node for node in result["nodes"]}
		self.assertIn("empresa:22223333", nodes_by_id)
		self.assertIn("socio:***999000**", nodes_by_id)
		self.assertNotIn("capital_social", nodes_by_id["empresa:22223333"]["data"])
		self.assertEqual(result["metadata"]["depth"], 2)
		self.assertEqual(result["metadata"]["total_edges"], 3)
		self.assertFalse(result["metadata"]["truncated"])
