"""The CNPJ transform: Latin-1 ZIP extraction and the DuckDB cleaning queries."""

import zipfile
from datetime import date
from decimal import Decimal

import pytest

from cleaners import build_empresas_query, build_reference_query, build_socios_query
from tasks.transform import extract_csv, parquet_name, silver_key


def test_extract_csv_decodes_latin1_including_bytes_duckdb_rejects(tmp_path):
    # 0x1A and 0x8F are valid Latin-1 that DuckDB's latin-1 reader refuses; accents must survive.
    raw = '"12345678";"AÇÃO É ÓTIMA"\n'.encode("latin-1") + b'"87654321";"CTRL\x1a AND \x8f"\n'
    zip_path = tmp_path / "Empresas0.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("K3241.K03200Y0.D61010.EMPRECSV", raw)

    csv_path = tmp_path / "out.csv"
    extract_csv(zip_path, csv_path)

    assert csv_path.read_text(encoding="utf-8") == '"12345678";"AÇÃO É ÓTIMA"\n"87654321";"CTRL\x1a AND \x8f"\n'


def test_extract_csv_requires_exactly_one_file(tmp_path):
    zip_path = tmp_path / "Socios0.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("a.csv", "x")
        archive.writestr("b.csv", "y")

    with pytest.raises(ValueError, match="expected one CSV"):
        extract_csv(zip_path, tmp_path / "out.csv")


@pytest.mark.parametrize("file_name, expected", [
    ("Empresas0.zip", "empresas_0.parquet"),
    ("Estabelecimentos9.zip", "estabelecimentos_9.parquet"),
    ("Socios3.zip", "socios_3.parquet"),
    ("Simples.zip", "simples.parquet"),
    ("Cnaes.zip", "cnaes.parquet"),
])
def test_parquet_name(file_name, expected):
    assert parquet_name(file_name) == expected


def test_parquet_name_rejects_unknown_files():
    with pytest.raises(ValueError):
        parquet_name("Unknown.zip")


@pytest.mark.parametrize("file_name, expected", [
    ("Empresas0.zip", "cnpj/silver/empresas/reference_month=2026-10/empresas_0.parquet"),
    ("Simples.zip", "cnpj/silver/simples/reference_month=2026-10/simples.parquet"),
    ("Cnaes.zip", "cnpj/silver/cnaes/reference_month=2026-10/cnaes.parquet"),
])
def test_silver_key(file_name, expected):
    assert silver_key("2026-10", file_name) == expected


def test_empresas_cleaning(run_query):
    rows = run_query(build_empresas_query, [
        '"01234567";"acme ltda";"2062";"5";"1.234.567,89";"3";""',
        '"12345678";"  Beta SA ";"0";"00";"abc";"07";"UNIÃO"',
        '"1234567";"seven digits";"2062";"49";"1,00";"01";""',       # not 8 characters: dropped
        '"";"no cnpj";"2062";"49";"1,00";"01";""',                    # dropped
    ])

    assert len(rows) == 2
    acme, beta = sorted(rows, key=lambda r: r["cnpj_basico"])
    assert acme == {
        "cnpj_basico": "01234567",
        "razao_social": "ACME LTDA",
        "natureza_juridica": 2062,
        "qualificacao_responsavel": "05",
        "capital_social": Decimal("1234567.89"),   # Brazilian thousands and decimal separators
        "porte_empresa": "03",
        "ente_federativo_responsavel": None,
    }
    assert beta["razao_social"] == "BETA SA"
    assert beta["natureza_juridica"] is None        # 0 means not informed
    assert beta["qualificacao_responsavel"] is None  # '00' means not informed
    assert beta["capital_social"] == Decimal("0")    # unparseable value
    assert beta["porte_empresa"] == "00"             # '07' is not a valid size code
    assert beta["ente_federativo_responsavel"] == "UNIÃO"


def test_socios_cleaning(run_query):
    rows = run_query(build_socios_query, [
        '"12345678";"2";"maria da silva";"***123456**";"49";"20210315";"";"***000000**";"";"00";"5"',
        '"12345678";"1";"holding sa";"87654321000110";"22";"00000000";"105";"";"";"";"0"',
    ])

    person, company = sorted(rows, key=lambda r: r["identificador_socio"], reverse=True)
    assert person["nome_socio_razao_social"] == "MARIA DA SILVA"
    assert person["cpf_cnpj_socio"] == "***123456**"
    assert person["qualificacao_socio"] == "49"
    assert person["data_entrada_sociedade"] == date(2021, 3, 15)
    assert person["pais"] is None
    assert person["qualificacao_representante_legal"] is None
    assert person["faixa_etaria"] == 5
    assert company["cpf_cnpj_socio"] == "87654321000110"
    assert company["data_entrada_sociedade"] is None   # 00000000 means no date
    assert company["pais"] == "105"


@pytest.mark.parametrize("ref_type, line, expected", [
    ("cnaes", '"6463800";"Outras sociedades de participação, exceto holdings"',
     {"cnae_fiscal": "6463800", "descricao": "Outras sociedades de participação, exceto holdings"}),
    ("motivos", '"1";"Extinção por encerramento liquidação voluntária"',
     {"codigo": "01", "descricao": "Extinção por encerramento liquidação voluntária"}),
    ("municipios", '"1";"Guaíba"', {"codigo": "0001", "nome": "Guaíba"}),
    ("naturezas", '"1015";"Órgão Público do Poder Executivo Federal"',
     {"codigo": 1015, "descricao": "Órgão Público do Poder Executivo Federal"}),
    ("paises", '"13";"Afeganistão"', {"codigo": "013", "nome": "Afeganistão"}),
    ("qualificacoes", '"5";"Administrador"', {"codigo": "05", "descricao": "Administrador"}),
])
def test_reference_tables(run_query, ref_type, line, expected):
    assert run_query(build_reference_query, [line], ref_type) == [expected]
