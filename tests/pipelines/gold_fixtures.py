"""Shared fixture builders for the dbt gold tests (silver tree + dbt invocation)."""

import json
import os
import subprocess
from pathlib import Path

from cleaners import (
    build_empresas_query,
    build_estabelecimentos_query,
    build_reference_query,
    build_simples_query,
    build_socios_query,
)

DBT_DIR = Path(__file__).resolve().parents[2] / "pipelines" / "dbt"

MONTHS = ["2026-08", "2026-09", "2026-10"]
LATEST = "2026-10"

GOLD_TABLES = (
    "empresa",
    "estabelecimento",
    "socio",
    "socio_history",
    "company_search",
    "simples_nacional",
    "cnae",
    "municipio",
    "motivo_situacao_cadastral",
    "natureza_juridica",
    "pais",
    "qualificacao_socio",
)


def write_parquet(con, builder, lines, out, *args):
    csv = out.parent / (out.name + ".csv")
    csv.write_text("\n".join(lines) + "\n", encoding="utf-8")
    con.execute(builder(str(csv), str(out), *args))


def est_line(cnpj_basico, ordem, dv, matriz_filial, situacao="02", nome_fantasia="ACME FANTASIA"):
    fields = [
        cnpj_basico, ordem, dv, str(matriz_filial), nome_fantasia, situacao, "20210315",
        "0", "", "", "20210101", "6463800", "", "RUA", "DAS FLORES", "10", "",
        "CENTRO", "90010001", "RS", "0001", "", "", "", "", "", "",
        "contato@acme.com", "", "",
    ]
    return ";".join(f'"{f}"' for f in fields)


def build_silver(root, con):
    """Write a silver tree for three months under root/cnpj/silver."""
    silver = root / "cnpj" / "silver"

    def table(entity, name, builder, lines, month, *args):
        dest = silver / entity / f"reference_month={month}" / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(con, builder, lines, dest, *args)

    empresas = {
        "2026-08": [
            '"01234567";"acme ltda";"2062";"5";"1.234.567,89";"3";""',
            '"12345678";"beta sa";"2062";"49";"100,00";"01";""',
        ],
        "2026-09": [
            '"01234567";"acme ltda";"2062";"5";"1.234.567,89";"3";""',
            '"12345678";"beta sa";"2062";"49";"100,00";"01";""',
        ],
        "2026-10": [
            '"01234567";"acme ltda";"2062";"5";"1.234.567,89";"3";""',
            # The Receita sometimes repeats a key with an empty placeholder row.
            '"01234567";"";"";"";"";"";""',
        ],
    }
    socios = {
        "2026-08": [
            '"01234567";"2";"maria da silva";"***123456**";"49";"20210315";"";"***000000**";"";"00";"5"',
            '"01234567";"1";"holding sa";"87654321000110";"22";"00000000";"105";"";"";"";"0"',
            '"12345678";"2";"joao pereira";"***999999**";"49";"20200101";"";"";"";"00";"4"',
        ],
        "2026-09": [
            '"01234567";"1";"holding sa";"87654321000110";"22";"00000000";"105";"";"";"";"0"',
        ],
        "2026-10": [
            '"01234567";"2";"maria da silva";"***123456**";"49";"20210315";"";"***000000**";"";"00";"5"',
            '"01234567";"1";"holding sa";"87654321000110";"22";"00000000";"105";"";"";"";"0"',
            # Exact duplicate of a partnership row.
            '"01234567";"1";"holding sa";"87654321000110";"22";"00000000";"105";"";"";"";"0"',
            '"01234567";"2";"carla souza";"***777777**";"49";"20261001";"";"";"";"00";"3"',
        ],
    }
    simples = {
        "2026-08": ['"01234567";"S";"20210101";"00000000";"N";"00000000";"00000000"',
                   '"12345678";"N";"00000000";"00000000";"N";"00000000";"00000000"'],
        "2026-09": ['"01234567";"S";"20210101";"00000000";"N";"00000000";"00000000"',
                   '"12345678";"N";"00000000";"00000000";"N";"00000000";"00000000"'],
        "2026-10": ['"01234567";"S";"20210101";"00000000";"N";"00000000";"00000000"'],
    }
    estabelecimentos = {
        "2026-08": [
            est_line("01234567", "0001", "11", 1),
            est_line("01234567", "0002", "38", 2, nome_fantasia="ACME FILIAL"),
        ],
        "2026-09": [
            est_line("01234567", "0001", "11", 1),
            est_line("01234567", "0002", "38", 2, nome_fantasia="ACME FILIAL"),
            est_line("12345678", "0001", "22", 1, nome_fantasia="BETA"),
        ],
        "2026-10": [
            est_line("01234567", "0001", "11", 1),
            est_line("01234567", "0002", "38", 2, nome_fantasia="ACME FILIAL"),
        ],
    }
    refs = {
        "cnaes": "cnaes.parquet",
        "motivos": "motivos.parquet",
        "municipios": "municipios.parquet",
        "naturezas": "naturezas.parquet",
        "paises": "paises.parquet",
        "qualificacoes": "qualificacoes.parquet",
    }
    ref_lines = {
        "cnaes": ['"6463800";"Outras sociedades de participação, exceto holdings"',
                  '"0111301";"Cultivo de arroz"'],
        "motivos": ['"1";"Extinção por encerramento liquidação voluntária"'],
        "municipios": ['"1";"Guaíba"', '"9050";"Florianópolis"'],
        "naturezas": ['"2062";"Sociedade Empresária Limitada"'],
        "paises": ['"105";"Paraguai"', '"13";"Afeganistão"'],
        "qualificacoes": ['"49";"Sócio-Administrador"', '"22";"Sócio"'],
    }

    for month in MONTHS:
        table("empresas", "empresas_1.parquet", build_empresas_query, empresas[month], month)
        table("estabelecimentos", "estabelecimentos_1.parquet", build_estabelecimentos_query, estabelecimentos[month], month)
        table("socios", "socios_1.parquet", build_socios_query, socios[month], month)
        table("simples", "simples.parquet", build_simples_query, simples[month], month)
        for ref_type, name in refs.items():
            table(ref_type, name, build_reference_query, ref_lines[ref_type], month, ref_type)


def make_storage(tmp_path):
    """Create an empty storage root with the gold target directories."""
    root = tmp_path / "storage"
    (root / "dbt").mkdir(parents=True, exist_ok=True)
    for table in GOLD_TABLES:
        (root / "cnpj" / "gold" / table).mkdir(parents=True, exist_ok=True)
    return root


def run_dbt(tmp_path, root, *args, reference_month=None):
    env = {
        **os.environ,
        "DBT_STORAGE_ROOT": str(root),
        "DBT_DUCKDB_PATH": str(root / "dbt" / "gold.duckdb"),
        "DBT_DUCKDB_TEMP_DIR": str(tmp_path / "duckdb_tmp"),
    }
    cmd = [
        "dbt", *args,
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target-path", str(tmp_path / "dbt-target"),
        "--log-path", str(tmp_path / "dbt-logs"),
        "--no-use-colors",
    ]
    if reference_month:
        cmd += ["--vars", json.dumps({"reference_month": reference_month})]
    return subprocess.run(cmd, env=env, capture_output=True, text=True)
