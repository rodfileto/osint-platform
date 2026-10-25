"""
FINEP spreadsheets → finep.* tables.

FINEP publishes two workbooks (Contratacao.xlsx and Liberacao.xlsx) whose sheet names, header
row and column order change between releases. Each table below lists the sheets that can feed
it and the header names each field has gone by; sheets and headers are matched after
normalizing accents and punctuation, and the header row is found by scanning.
"""

import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def normalize(text) -> str:
    """'Município Proponente' → 'municipio_proponente', 'Nº Parcela' → 'no_parcela'."""
    ascii_text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


def _str(value) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _number(value) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _int(value) -> Optional[int]:
    number = _number(value)
    return int(number) if number is not None else None


def _count(value) -> int:
    return _int(value) or 0


def _date(value):
    return value.date() if hasattr(value, "date") else None


def _bool(value) -> Optional[bool]:
    text = normalize(value) if value is not None else ""
    return {"sim": True, "s": True, "nao": False, "n": False}.get(text)


@dataclass(frozen=True)
class Table:
    dataset: str                       # 'contratacao' or 'liberacao'
    sheets: dict[str, dict]            # normalized sheet name → default field values
    fields: dict[str, tuple]           # column → (cleaner, header aliases...)
    key: str                           # rows without this field are separators/subtotals
    defaults: dict = field(default_factory=dict)


PROJECT_FIELDS = {
    "instrumento": (_str, "instrumento"),
    "demanda": (_str, "demanda"),
    "ref": (_str, "ref"),
    "contrato": (_str, "contrato"),
    "data_assinatura": (_date, "data_assinatura"),
    "prazo_execucao": (_date, "prazo_execucao"),
    "titulo": (_str, "titulo"),
    "proponente": (_str, "proponente"),
    "cnpj_proponente": (_str, "cnpj_proponente"),
    "executor": (_str, "executor"),
    "municipio": (_str, "municipio", "municipio_proponente"),
    "uf": (_str, "uf", "uf_proponente"),
    "valor_finep": (_number, "valor_finep"),
    "status": (_str, "status"),
    "contrapartida_financeira": (_number, "contrapartida_financeira"),
    "contrapartida_nao_financeira": (_number, "contrapartida_nao_financeira"),
    "valor_pago": (_number, "valor_pago"),
    "intervenientes": (_count, "intervenientes"),
    "aporte_financeiro_interv": (_number, "aporte_financeiro_interv"),
    "aporte_nao_financeiro_interv": (_number, "aporte_nao_financeiro_interv"),
}

DISBURSEMENT_FIELDS = {
    "ref": (_str, "ref"),
    "contrato": (_str, "contrato"),
    "num_parcela": (_int, "no_parcela"),
    "num_liberacao": (_int, "no_liberacao"),
    "data_liberacao": (_date, "data_liberacao"),
    "valor_liberado": (_number, "valor_liberado"),
}

# Until early 2026 the "Operação Direta" sheet held every direct instrument; FINEP now splits it.
DIRECT_SHEETS = {
    "projetos_operacao_direta": {},
    "projetos_nao_reembolsavel_icts": {"instrumento": "Não Reembolsável a ICTs"},
    "projetos_credito_direto": {"instrumento": "Crédito Direto"},
    "projetos_subvencao_direta": {},
    "projetos_subv_direta_agentes": {},
}

TABLES = {
    "projetos_operacao_direta": Table(
        dataset="contratacao",
        sheets=DIRECT_SHEETS,
        fields={**PROJECT_FIELDS,
                "prazo_execucao_original": (_date, "prazo_execucao_original"),
                "cnpj_executor": (_str, "cnpj_executor"),
                "resumo_publicavel": (_str, "resumo_publicavel")},
        key="contrato",
    ),
    "projetos_credito_descentralizado": Table(
        dataset="contratacao",
        sheets={"proj_credito_descentralizado": {}, "projetos_cred_descentralizado": {}},
        fields={
            "data_assinatura": (_date, "data_assinatura"),
            "contrato": (_str, "contrato", "contrato_finep_agente"),
            "proponente": (_str, "proponente", "beneficiario"),
            "cnpj_proponente": (_str, "cnpj_proponente", "cnpj_beneficiario"),
            "uf": (_str, "uf", "uf_beneficiario"),
            "valor_financiado": (_number, "valor_financiado"),
            "valor_liberado": (_number, "valor_liberado"),
            "contrapartida": (_number, "contrapartida"),
            "outros_recursos": (_number, "outros_recursos"),
            "agente_financeiro": (_str, "agente_financeiro_responsavel_pela_operacao", "agente"),
        },
        key="contrato",
    ),
    "projetos_investimento": Table(
        dataset="contratacao",
        sheets={"projetos_investimento": {}, "investimento_direto_startups": {}},
        fields={
            "ref": (_str, "ref", "refe"),
            "numero_contrato": (_str, "numero_do_contrato"),
            "cnpj_proponente": (_str, "cnpj_proponente", "cnpj"),
            "proponente": (_str, "proponente", "razao_social"),
            "data_assinatura": (_date, "data_assinatura", "data_da_contratacao"),
            "data_follow_on": (_date, "data_follow_on"),
            "valor_follow_on": (_number, "valor_follow_on"),
            "valor_total_contratado": (_number, "valor_total_contratado"),
            "valor_total_liberado": (_number, "valor_total_liberado"),
            "opcao_compra_exercida": (_bool, "opcao_de_compra_exercida", "exercer_opcao_de_compra"),
            "opcao_compra_prorrogada": (_bool, "opcao_de_compra_prorrogada", "prorrogar_opcao_de_compra"),
            "data_exercicio_opcao": (_date, "data_do_exercicio_da_opcao"),
            "valuation_opcao": (_number, "valuation_aplicado_opcao_r"),
            "participacao_finep": (_number, "participacao_da_finep"),
            "desinvestimento_realizado": (_bool, "desinvestimento_realizado", "realizar_desinvestimento"),
            "data_desinvestimento": (_date, "data_do_desinvestimento"),
            "valor_desinvestimento": (_number, "valor_do_desinvestimento"),
            "tir": (_number, "tir"),
            "moi": (_number, "moi"),
        },
        key="numero_contrato",
    ),
    "projetos_ancine": Table(
        dataset="contratacao",
        sheets={"projetos_ancine": {}},
        fields=PROJECT_FIELDS,
        key="contrato",
    ),
    "liberacoes_operacao_direta": Table(
        dataset="liberacao",
        sheets=DIRECT_SHEETS,
        fields=DISBURSEMENT_FIELDS,
        key="contrato",
    ),
    "liberacoes_credito_descentralizado": Table(
        dataset="liberacao",
        sheets={"proj_credito_descentralizado": {}, "projetos_cred_descentralizado": {}},
        fields={
            "contrato": (_str, "contrato"),
            "cnpj_proponente": (_str, "cnpj_proponente", "cnpj_beneficiario"),
            "num_parcela": (_int, "numero_da_parcela"),
            "data_liberacao": (_date, "data_da_liberacao"),
            "valor_liberado": (_number, "valor_liberado"),
        },
        key="contrato",
    ),
    "liberacoes_ancine": Table(
        dataset="liberacao",
        sheets={"projetos_ancine": {}},
        fields=DISBURSEMENT_FIELDS,
        key="contrato",
    ),
}


def _header_row(rows: list[tuple], key_aliases: tuple) -> int:
    for index, row in enumerate(rows):
        if any(normalize(cell) in key_aliases for cell in row if cell is not None):
            return index
    raise ValueError(f"no header row containing {key_aliases} in the first {len(rows)} rows")


def read_rows(xlsx_path: Path, spec: Table) -> list[dict]:
    """Every data row of every sheet that feeds the table, as {column: value}."""
    import openpyxl

    workbook = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    try:
        sheets = {normalize(ws.title): ws for ws in workbook.worksheets}
        found = [name for name in spec.sheets if name in sheets]
        if not found:
            raise ValueError(f"{xlsx_path.name}: none of the sheets {list(spec.sheets)} exist (has {list(sheets)})")

        records = []
        key_aliases = spec.fields[spec.key][1:]
        for sheet_name in found:
            rows = sheets[sheet_name].iter_rows(values_only=True)
            head = [next(rows, ()) for _ in range(15)]
            header_index = _header_row(head, key_aliases)
            headers = {}
            for position, cell in enumerate(head[header_index]):
                if cell is not None:
                    headers.setdefault(normalize(cell), position)
            columns = {}
            for column, (cleaner, *aliases) in spec.fields.items():
                position = next((headers[a] for a in aliases if a in headers), None)
                columns[column] = (cleaner, position)
            missing = [c for c, (_, p) in columns.items() if p is None and c not in spec.sheets[sheet_name]]
            if spec.key in missing:
                raise ValueError(f"{sheet_name}: key column {spec.key} not found")
            if missing:
                logger.info(f"  {sheet_name}: no column for {missing} (left empty)")

            before = len(records)
            data_rows = head[header_index + 1:] + list(rows)
            for row in data_rows:
                record = {c: (cleaner(row[p]) if p is not None and p < len(row) else None)
                          for c, (cleaner, p) in columns.items()}
                for column, value in spec.sheets[sheet_name].items():
                    if column in record:
                        record[column] = record[column] or value
                if record[spec.key]:
                    records.append(record)
            logger.info(f"  {sheet_name}: {len(records) - before:,} rows (header on row {header_index + 1})")
        return records
    finally:
        workbook.close()


def _connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def latest_download(cur, dataset: str) -> Optional[tuple[int, Path]]:
    cur.execute(
        """
        SELECT id, local_path FROM finep.download_manifest
        WHERE dataset_type = %s AND processing_status <> 'failed' AND local_path IS NOT NULL
        ORDER BY download_date DESC
        """,
        (dataset,),
    )
    return next(((mid, Path(p)) for mid, p in cur.fetchall() if Path(p).exists()), None)


def load_table(table: str, force: bool = False) -> int:
    """Replace finep.<table> with the newest spreadsheet, unless it already holds that file."""
    spec = TABLES[table]
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            download = latest_download(cur, spec.dataset)
            if download is None:
                raise RuntimeError(f"No {spec.dataset} spreadsheet downloaded yet")
            manifest_id, path = download
            cur.execute(f"SELECT count(*) FROM finep.{table} WHERE manifest_id = %s", (manifest_id,))
            if cur.fetchone()[0] and not force:
                logger.info(f"finep.{table} already holds manifest {manifest_id} ({path.name}); nothing to do")
                return 0

            logger.info(f"Loading finep.{table} from {path.name} (manifest {manifest_id})")
            records = read_rows(path, spec)
            if not records:
                raise ValueError(f"{path.name} produced no rows for finep.{table}")
            columns = list(spec.fields) + ["manifest_id"]
            if table == "projetos_operacao_direta":
                cur.execute("SELECT count(*) FROM finep.nlp_document_topic")
                if cur.fetchone()[0]:
                    logger.warning("Reloading removes the NLP topic assignments; re-run finep_nlp_topic afterwards")
            cur.execute(f"DELETE FROM finep.{table}")
            execute_values(
                cur,
                f"INSERT INTO finep.{table} ({', '.join(columns)}) VALUES %s",
                [tuple(r[c] for c in spec.fields) + (manifest_id,) for r in records],
                page_size=1000,
            )
            cur.execute("UPDATE finep.download_manifest SET processing_status = 'loaded', updated_at = now() WHERE id = %s",
                        (manifest_id,))
        logger.info(f"finep.{table}: {len(records):,} rows")
        return len(records)
    finally:
        conn.close()
