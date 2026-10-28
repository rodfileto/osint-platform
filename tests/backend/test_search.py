"""Unified company search over the dev seed's search views."""

import pytest

from api.routers.search import _search_postgres_companies


def search(run, query):
    return run(lambda db: _search_postgres_companies(db, query, 10))


@pytest.mark.parametrize("query", ["acme", "ACME INOVAÇÃO", "12345678", "1234"])
def test_finds_company_by_name_or_cnpj(run, query):
    result = search(run, query)

    assert "12345678" in {r.cnpj_basico for r in result.results}


def test_no_match(run):
    assert search(run, "zzzz-no-such-company").total == 0


def test_active_companies_come_first(run):
    # "LTDA" matches active seed companies and the inactive EMPRESA DESATIVADA SERVICOS LTDA.
    situacoes = [r.situacao_cadastral for r in search(run, "LTDA").results]

    assert "02" in situacoes and any(s != "02" for s in situacoes)
    first_inactive = next(i for i, s in enumerate(situacoes) if s != "02")
    assert all(s != "02" for s in situacoes[first_inactive:])


def test_company_search_count_is_capped(run, monkeypatch):
    from starlette.requests import Request

    from api.routers import cnpj
    from api.services import company_search

    monkeypatch.setattr(company_search, "COUNT_CAP", 1)
    request = Request({
        "type": "http", "method": "GET", "path": "/api/cnpj/search", "query_string": b"",
        "headers": [], "server": ("test", 80), "scheme": "http", "root_path": "",
    })
    page = run(lambda db: cnpj._fetch_company_search_page(db, request, True, None, None, None, None, 1, 20))

    assert page.count == 1
    assert page.count_capped is True
    assert page.next is None
    assert len(page.results) > 1, "the page itself is not limited by the cap"


def test_company_search_page_without_name_walk(run, monkeypatch):
    # A walk too short to fill the page must fall back to sorting every match.
    from starlette.requests import Request

    from api.routers import cnpj
    from api.services import company_search

    request = Request({
        "type": "http", "method": "GET", "path": "/api/cnpj/search", "query_string": b"",
        "headers": [], "server": ("test", 80), "scheme": "http", "root_path": "",
    })

    def page():
        return run(lambda db: cnpj._fetch_company_search_page(db, request, True, None, None, None, None, 1, 20))

    expected = [r.cnpj_14 for r in page().results]
    monkeypatch.setattr(company_search, "COUNT_CAP", 1)
    monkeypatch.setattr(company_search, "NAME_WALK", 1)
    assert [r.cnpj_14 for r in page().results] == expected
