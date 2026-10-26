"""The FINEP loader finds columns by header name, whatever row the header is on."""

from datetime import datetime

import openpyxl
import pytest

from finep.loader import TABLES, normalize, read_rows


@pytest.mark.parametrize("text, expected", [
    ("Município Proponente", "municipio_proponente"),
    ("Nº Parcela", "no_parcela"),
    ("  Valor FINEP (R$) ", "valor_finep_r"),
    ("Projetos Operação Direta", "projetos_operacao_direta"),
])
def test_normalize(text, expected):
    assert normalize(text) == expected


def _workbook(path, sheets):
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    workbook.save(path)


def test_read_rows_matches_headers_by_name(tmp_path):
    path = tmp_path / "Contratacao.xlsx"
    _workbook(path, {
        # 2026 layout: title lines above the header, accents and a renamed municipality column.
        "Projetos Operação Direta": [
            ["FINEP - Projetos contratados"],
            [],
            ["Contrato", "Título", "Valor FINEP", "Data Assinatura", "Município Proponente", "UF"],
            ["01.23.0456.00", "Sensor óptico", 150000.5, datetime(2024, 5, 2), "Campinas", "SP"],
            [None, "Total", 150000.5, None, None, None],   # subtotal row: no contract, skipped
        ],
        # Older sheet with the header on the first row and no instrument column.
        "Projetos Crédito Direto": [
            ["Contrato", "Título", "Valor FINEP", "Município", "UF"],
            ["02.24.0001.00", "Planta piloto", "not a number", "Recife", "PE"],
        ],
    })

    rows = read_rows(path, TABLES["projetos_operacao_direta"])

    assert [r["contrato"] for r in rows] == ["01.23.0456.00", "02.24.0001.00"]
    first, second = rows
    assert first["titulo"] == "Sensor óptico"
    assert first["valor_finep"] == 150000.5
    assert first["data_assinatura"].isoformat() == "2024-05-02"
    assert first["municipio"] == "Campinas"          # matched through the alias municipio_proponente
    assert first["resumo_publicavel"] is None         # column absent from the sheet
    assert second["municipio"] == "Recife"
    assert second["valor_finep"] is None              # unparseable number
    assert second["instrumento"] == "Crédito Direto"  # sheet default fills the missing column


def test_read_rows_fails_without_the_key_column(tmp_path):
    path = tmp_path / "Contratacao.xlsx"
    _workbook(path, {"Projetos Operação Direta": [["Título", "Valor FINEP"], ["x", 1.0]]})

    with pytest.raises(ValueError, match="no header row"):
        read_rows(path, TABLES["projetos_operacao_direta"])
