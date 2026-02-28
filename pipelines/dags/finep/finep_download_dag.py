"""
FINEP Download DAG

Coleta periódica dos arquivos Excel públicos disponibilizados pela FINEP:
  - http://download.finep.gov.br/Contratacao.xlsx
  - http://download.finep.gov.br/Liberacao.xlsx

Cada execução verifica via HEAD request se os arquivos mudaram remotamente
antes de realizar o download, evitando coletas desnecessárias.
O resultado de cada coleta é registrado em finep.download_manifest.

Fluxo por dataset
─────────────────
check_remote_headers
        │
        ▼
  [changed?] ──não──→ skip (log + XCom)
        │
       sim
        │
        ▼
  download_file
        │
        ▼
  validate_xlsx
        │
        ▼
  register_manifest
        │
        ▼
  generate_report

Schedule: mensal, dia 2 às 06:00 UTC — mesmo dia do CNPJ (02:00 UTC),
          com 4 h de margem para não concorrer por recursos.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Adiciona o diretório de scripts ao path do Airflow worker ──────────────
sys.path.insert(0, "/opt/airflow/scripts/finep")

from downloader import (
    FINEP_DATASETS,
    collect_dataset,
    has_remote_changed,
    get_last_download_info,
)

logger = logging.getLogger(__name__)

# ── Constantes ─────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("/opt/airflow/data/finep")

# ── Default args ────────────────────────────────────────────────────────────
default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


# ============================================================================
# Callables das tasks
# ============================================================================

def _check_dataset(dataset_type: str, url: str, **context) -> bool:
    """
    Verifica via HEAD se o arquivo remoto mudou.

    Retorna True  → prossegue com o download
    Retorna False → ShortCircuitOperator ignora tasks downstream
    """
    force = context["dag_run"].conf.get("force", False)
    if force:
        logger.info(f"[{dataset_type}] Modo force=True — ignorando verificação de mudança.")
        context["task_instance"].xcom_push(key="remote_headers", value={})
        return True

    try:
        changed, remote_headers = has_remote_changed(dataset_type, url)
    except Exception as exc:
        # Falha no HEAD → procede com o download por precaução
        logger.warning(
            f"[{dataset_type}] HEAD request falhou ({exc}), forçando download."
        )
        changed = True
        remote_headers = {}

    context["task_instance"].xcom_push(key="remote_headers", value={
        "etag":           remote_headers.get("etag"),
        "last_modified":  str(remote_headers.get("last_modified") or ""),
        "content_length": remote_headers.get("content_length"),
    })

    if not changed:
        logger.info(
            f"[{dataset_type}] Arquivo inalterado — tasks downstream serão ignoradas."
        )
    return changed


def _download_and_register(dataset_type: str, url: str, **context) -> dict:
    """
    Orquestra download + validação + registro no manifesto.

    Retorna dict com resultado que é armazenado em XCom.
    """
    force = context["dag_run"].conf.get("force", False)
    result = collect_dataset(
        dataset_type=dataset_type,
        url=url,
        output_dir=OUTPUT_DIR,
        force=force,
    )
    logger.info(f"[{dataset_type}] Resultado: {result}")
    return result


def _generate_report(**context) -> None:
    """
    Gera um relatório consolidado com o status de ambos os datasets.
    Exibe informações da última coleta registrada no manifesto.
    """
    ti = context["task_instance"]
    report_lines = [
        "=" * 60,
        "  FINEP Download Report",
        f"  Data/hora: {datetime.utcnow().isoformat(timespec='seconds')} UTC",
        "=" * 60,
    ]

    for dataset_type in FINEP_DATASETS:
        # Tenta ler o resultado do XCom da task de download deste dataset
        result = ti.xcom_pull(task_ids=f"download_{dataset_type}", key="return_value")

        last = get_last_download_info(dataset_type)

        if result and result.get("skipped"):
            status_line = "↷  Ignorado (arquivo inalterado)"
        elif result:
            status_line = (
                f"✓  Baixado  |  {result.get('file_size_bytes', 0):,} bytes  "
                f"|  {result.get('rows_count', '?')} linhas  "
                f"|  SHA-256: {(result.get('checksum') or '')[:16]}..."
            )
        else:
            status_line = "✗  Sem resultado nesta execução"

        report_lines += [
            "",
            f"  Dataset : {dataset_type.upper()}",
            f"  Status  : {status_line}",
        ]

        if last:
            report_lines.append(
                f"  Último  : {last['download_date'].isoformat(timespec='seconds')} UTC"
            )

    report_lines += ["", "=" * 60]
    report = "\n".join(report_lines)
    logger.info("\n" + report)
    print(report)


# ============================================================================
# DAG
# ============================================================================

with DAG(
    dag_id="finep_download",
    default_args=default_args,
    description="Download periódico de Contratacao.xlsx e Liberacao.xlsx da FINEP",
    schedule_interval="0 6 2 * *",   # mensal: dia 2 de cada mês às 06:00 UTC (após o CNPJ às 02:00)
    catchup=False,
    max_active_runs=1,
    params={
        "force": False,   # passar force=True via conf para forçar re-download
    },
    tags=["finep", "download", "xlsx"],
) as dag:

    # ── Uma task group por dataset ──────────────────────────────────────────
    # Usamos tasks separadas (sem TaskGroup) para manter simplicidade e
    # visibilidade individual no Airflow UI.

    all_download_tasks = []

    for _dataset_type, _url in FINEP_DATASETS.items():

        # Task 1: verificar se o arquivo remoto mudou
        check_task = ShortCircuitOperator(
            task_id=f"check_{_dataset_type}",
            python_callable=_check_dataset,
            op_kwargs={"dataset_type": _dataset_type, "url": _url},
            provide_context=True,
            # ignore_downstream_trigger_rules=True garante que tasks
            # downstream dentro do mesmo dataset sejam puladas quando
            # ShortCircuit retorna False, sem afetar outras branches.
            ignore_downstream_trigger_rules=False,
        )

        # Task 2: download + validação + registro no manifesto
        download_task = PythonOperator(
            task_id=f"download_{_dataset_type}",
            python_callable=_download_and_register,
            op_kwargs={"dataset_type": _dataset_type, "url": _url},
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
        )

        check_task >> download_task
        all_download_tasks.append(download_task)

    # Task final: relatório consolidado
    # trigger_rule=ALL_DONE garante que o relatório rode mesmo que algum
    # dataset tenha sido ignorado (ShortCircuit)
    from airflow.utils.trigger_rule import TriggerRule

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Triggers de carga — todas disparam em paralelo após o relatório ────
    # Cada aba do Contratacao.xlsx tem sua própria DAG de carga.
    # wait_for_completion=False: a download DAG finaliza sem aguardar as cargas.

    trigger_load_operacao_direta = TriggerDagRunOperator(
        task_id="trigger_load_operacao_direta",
        trigger_dag_id="finep_load_operacao_direta",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_credito = TriggerDagRunOperator(
        task_id="trigger_load_credito_descentralizado",
        trigger_dag_id="finep_load_credito_descentralizado",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_investimento = TriggerDagRunOperator(
        task_id="trigger_load_investimento",
        trigger_dag_id="finep_load_investimento",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_ancine = TriggerDagRunOperator(
        task_id="trigger_load_ancine",
        trigger_dag_id="finep_load_ancine",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for _download_task in all_download_tasks:
        _download_task >> report_task

    # Triggers de carga para Liberacao.xlsx
    trigger_load_lib_operacao_direta = TriggerDagRunOperator(
        task_id="trigger_load_liberacoes_operacao_direta",
        trigger_dag_id="finep_load_liberacoes_operacao_direta",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_lib_credito = TriggerDagRunOperator(
        task_id="trigger_load_liberacoes_credito_descentralizado",
        trigger_dag_id="finep_load_liberacoes_credito_descentralizado",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_lib_ancine = TriggerDagRunOperator(
        task_id="trigger_load_liberacoes_ancine",
        trigger_dag_id="finep_load_liberacoes_ancine",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    report_task >> [
        # Contratacao.xlsx
        trigger_load_operacao_direta,
        trigger_load_credito,
        trigger_load_investimento,
        trigger_load_ancine,
        # Liberacao.xlsx
        trigger_load_lib_operacao_direta,
        trigger_load_lib_credito,
        trigger_load_lib_ancine,
    ]


# ============================================================================
# DAG auxiliar: trigger manual com force=True
# ============================================================================

with DAG(
    dag_id="finep_download_force",
    default_args=default_args,
    description="Re-download forçado de todos os arquivos FINEP (trigger manual)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["finep", "download", "manual"],
) as dag_force:

    def _force_collect_all(**context) -> list:
        from downloader import collect_all
        results = collect_all(output_dir=OUTPUT_DIR, force=True)
        for r in results:
            logger.info(r)
        return results

    force_task = PythonOperator(
        task_id="force_download_all",
        python_callable=_force_collect_all,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )
