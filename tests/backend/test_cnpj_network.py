"""Ownership networks over the dev seed.

ACME INOVACAO (12345678) has two people (ANA PAULA TESTE, ROBERTO MENDES) and one company
(HOLDING EXEMPLO, 87654321) as partners. HOLDING has its own partner, CARLOS LIMA, and ANA is
also a partner of NOVA ENERGIA PESQUISA (11222333).
"""

from api.schemas.cnpj import CompanyNetworkResponse, PersonNetworkResponse
from api.services.cnpj_network import CnpjNetworkService

ACME, HOLDING, NOVA = "empresa:12345678", "empresa:87654321", "empresa:11222333"
ANA, ROBERTO, CARLOS = (
    "socio:***222333**:ANA PAULA TESTE",
    "socio:***333444**:ROBERTO MENDES",
    "socio:***666777**:CARLOS LIMA",
)


def company(run, cnpj, **kwargs):
    return run(lambda db: CnpjNetworkService(db).get_company_network(cnpj, **kwargs))


def person(run, cpf, nome, **kwargs):
    return run(lambda db: CnpjNetworkService(db).get_person_network(cpf, nome, **kwargs))


def edges(network):
    return {(e["source"], e["target"]) for e in network["edges"]}


def test_company_depth_1_has_direct_partners_including_a_company(run):
    network = company(run, "12345678", depth=1)

    CompanyNetworkResponse.model_validate(network)
    assert edges(network) == {(ANA, ACME), (ROBERTO, ACME), (HOLDING, ACME)}
    nodes = {n["id"]: n for n in network["nodes"]}
    assert nodes[ACME]["data"]["is_core"] is True
    assert nodes[HOLDING]["type"] == "empresa" and nodes[HOLDING]["label"] == "HOLDING EXEMPLO LTDA"
    assert nodes[ANA]["type"] == "socio"
    assert network["metadata"]["truncated"] is False


def test_company_depth_2_reaches_partners_of_partners_and_their_companies(run):
    network = company(run, "12345678", depth=2)

    assert edges(network) == {
        (ANA, ACME), (ROBERTO, ACME), (HOLDING, ACME),
        (CARLOS, HOLDING),   # partner of the corporate partner
        (ANA, NOVA),         # other company of a partner
    }
    assert network["metadata"]["total_nodes"] == 6


def test_company_network_follows_corporate_partnership_upwards(run):
    # HOLDING's network includes the company it is a partner in.
    network = company(run, "87654321", depth=1)

    assert edges(network) == {(CARLOS, HOLDING), (HOLDING, ACME)}


def test_max_edges_truncates_and_reports_it(run):
    network = company(run, "12345678", depth=2, max_edges=2)

    assert len(network["edges"]) == 2
    assert network["metadata"]["truncated"] is True
    assert network["metadata"]["max_edges"] == 2
    assert network["metadata"]["total_relationships"] == 3


def test_unknown_company_returns_none(run):
    assert company(run, "00000001", depth=1) is None


def test_person_network(run):
    network = person(run, "***222333**", "ANA PAULA TESTE", depth=1)

    PersonNetworkResponse.model_validate(network)
    core = "pessoa:***222333**:ANA PAULA TESTE"
    assert edges(network) == {(core, ACME), (core, NOVA)}
    assert network["metadata"]["core_nome"] == "ANA PAULA TESTE"


def test_person_depth_2_adds_co_partners(run):
    network = person(run, "***222333**", "ANA PAULA TESTE", depth=2)

    sources = {source for source, _ in edges(network)}
    assert "pessoa:***333444**:ROBERTO MENDES" in sources
    assert "empresa:87654321" in sources


def test_person_is_matched_by_cpf_and_name(run):
    assert person(run, "***222333**", "SOMEONE ELSE", depth=1) is None
