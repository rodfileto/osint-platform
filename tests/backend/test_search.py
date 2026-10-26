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
