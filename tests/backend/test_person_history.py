"""Person detail (current vs past partnerships) and the partner-history endpoint."""

from api.routers.cnpj import _fetch_partner_history, _fetch_person_detail


def person(run, nome, cpf):
    return run(lambda db: _fetch_person_detail(db, nome, cpf))


def history(run, cnpj):
    return run(lambda db: _fetch_partner_history(db, cnpj))


def test_person_detail_mixes_current_and_past_partnerships(run):
    detail = person(run, "ANA PAULA TESTE", "***222333**")

    assert detail.total_empresas == 3
    by_cnpj = {e.cnpj_basico: e for e in detail.empresas}

    # Current partnerships (latest month).
    assert by_cnpj["12345678"].is_current is True
    assert by_cnpj["11222333"].is_current is True
    # A past partnership, from socio_history.
    assert by_cnpj["44556677"].is_current is False
    assert by_cnpj["44556677"].reference_month == "2025-11"


def test_person_detail_past_only_person(run):
    detail = person(run, "CARLOS EDUARDO", "***123123**")

    assert detail.total_empresas == 2
    assert all(e.is_current is False for e in detail.empresas)
    by_cnpj = {e.cnpj_basico: e for e in detail.empresas}
    assert by_cnpj["99887766"].razao_social == "ESTUDIO AUDIOVISUAL BRASIL LTDA"
    # A company missing from the latest snapshot keeps its cnpj_basico, without a name.
    assert by_cnpj["55443322"].razao_social is None


def test_partner_history_endpoint(run):
    result = history(run, "99887766")

    assert result.total == 1
    assert result.history[0].nome_socio_razao_social == "CARLOS EDUARDO"
    assert result.history[0].is_current is False


def test_partner_history_empty_for_unknown_company(run):
    assert history(run, "00000001").total == 0
