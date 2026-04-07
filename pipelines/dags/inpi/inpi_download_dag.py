"""
INPI Patents Download DAG

Coleta periódica dos arquivos CSV de patentes publicados pelo INPI:
  https://dadosabertos.inpi.gov.br/index/patentes/

Datasets coletados (10 arquivos):
  patentes_dados_bibliograficos, patentes_conteudo, patentes_inventores,
  patentes_depositantes, patentes_classificacao_ipc, patentes_despachos,
  patentes_procuradores, patentes_prioridades, patentes_vinculos,
  patentes_renumeracoes

A mudança de cada arquivo é detectada via HEAD request (ETag / Last-Modified).
Se nenhum arquivo mudou desde o último download, a DAG termina sem realizar
downloads (ShortCircuitOperator).

Quando há mudança, cada CSV é enviado ao MinIO como snapshot imutável:
  s3://osint-raw/inpi/patentes/<YYYYMMDD>/<FILENAME>.csv

onde <YYYYMMDD> é derivado do header Last-Modified do arquivo remoto.
O histórico completo de snapshots é preservado — nenhum objeto é sobrescrito.

O resultado de cada coleta é registrado em inpi.download_manifest.

Fluxo
─────
check_any_changed  (HEAD × 10 arquivos)
        │
        ▼ (ShortCircuit — para se nada mudou)
download_<dataset>  (paralelo, um por arquivo)
        │
        ▼
generate_report

Schedule: semanal, segunda-feira às 05:00 UTC.
Parâmetros:
  force (bool) — forçar re-download mesmo que ETag/Last-Modified não mudem
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator, ShortCircuitOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

from inpi.downloader import (
    INPI_DATASETS,
    check_remote_headers,
    collect_dataset,
    get_last_download_info,
)

logger = logging.getLogger(__name__)


# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),  # first Monday of 2026
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=20),
}


# ============================================================================
# Callables das tasks
# ============================================================================

def _check_any_changed(**context) -> bool:
    """
    Faz HEAD em todos os datasets e verifica se algum mudou.

    Retorna True  → há novidades, prossegue com downloads
    Retorna False → ShortCircuitOperator encerra a DAG sem downloads
    """
    force = context["dag_run"].conf.get("force", False)
    if force:
        logger.info("force=True — ignorando verificação de mudança.")
        context["task_instance"].xcom_push(key="changed_datasets", value=list(INPI_DATASETS.keys()))
        return True

    changed = []
    for dataset_name, url in INPI_DATASETS.items():
        try:
            remote = check_remote_headers(dataset_name, url)
            last = get_last_download_info(dataset_name)

            if last is None:
                logger.info(f"[{dataset_name}] Sem histórico — será baixado.")
                changed.append(dataset_name)
                continue

            # Comparar ETag
            if remote.get("etag") and last.get("http_etag"):
                if remote["etag"] != last["http_etag"]:
                    logger.info(f"[{dataset_name}] ETag mudou — será baixado.")
                    changed.append(dataset_name)
                else:
                    logger.info(f"[{dataset_name}] ETag igual — ignorando.")
                continue

            # Fallback: Last-Modified
            if remote.get("last_modified") and last.get("http_last_modified"):
                if remote["last_modified"] > last["http_last_modified"]:
                    logger.info(f"[{dataset_name}] Last-Modified mais recente — será baixado.")
                    changed.append(dataset_name)
                else:
                    logger.info(f"[{dataset_name}] Last-Modified igual — ignorando.")
                continue

            # Sem informação suficiente → precaução
            logger.warning(f"[{dataset_name}] Sem ETag nem Last-Modified — será baixado.")
            changed.append(dataset_name)

        except Exception as exc:
            logger.warning(f"[{dataset_name}] HEAD falhou ({exc}) — será baixado por precaução.")
            changed.append(dataset_name)

    logger.info(f"{len(changed)}/{len(INPI_DATASETS)} datasets com mudanças: {changed}")
    context["task_instance"].xcom_push(key="changed_datasets", value=changed)
    return len(changed) > 0


def _download_dataset(dataset_name: str, url: str, **context) -> dict:
    """
    Baixa um dataset INPI e faz upload para MinIO.

    Datasets inalterados (não listados em changed_datasets) são ignorados
    com status 'skipped', sem erro.
    """
    force = context["dag_run"].conf.get("force", False)
    changed_datasets: list[str] = context["task_instance"].xcom_pull(
        task_ids="check_any_changed", key="changed_datasets"
    ) or []

    if not force and dataset_name not in changed_datasets:
        logger.info(f"[{dataset_name}] Não mudou — ignorando.")
        return {"dataset_name": dataset_name, "skipped": True, "reason": "unchanged"}

    result = collect_dataset(dataset_name=dataset_name, url=url, force=force)
    logger.info(f"[{dataset_name}] Resultado: {result}")
    return result


def _generate_report(**context) -> None:
    """Relatório consolidado do estado de todos os datasets após a execução."""
    ti = context["task_instance"]

    lines = [
        "=" * 65,
        "  INPI Patentes Download Report",
        f"  Data/hora: {datetime.utcnow().isoformat(timespec='seconds')} UTC",
        "=" * 65,
    ]

    changed: list[str] = ti.xcom_pull(task_ids="check_any_changed", key="changed_datasets") or []

    for dataset_name in INPI_DATASETS:
        result = ti.xcom_pull(task_ids=f"download_{dataset_name}", key="return_value")
        last = get_last_download_info(dataset_name)

        if result and result.get("skipped"):
            status_line = "↷  Ignorado (arquivo inalterado)"
        elif result and not result.get("skipped"):
            size_mb = result.get("file_size", 0) / 1024 / 1024
            status_line = (
                f"✓  Baixado  |  {size_mb:.1f} MB  "
                f"|  {result.get('row_count', '?')} linhas  "
                f"|  snapshot {result.get('snapshot_date', '?')}"
            )
        else:
            status_line = "✗  Sem resultado nesta execução"

        lines.append("")
        lines.append(f"  Dataset : {dataset_name}")
        lines.append(f"  Status  : {status_line}")
        if last:
            lines.append(
                f"  Último  : snapshot {last.get('snapshot_date')}  "
                f"|  {(last.get('file_size_bytes') or 0) / 1024 / 1024:.1f} MB"
            )

    lines += [
        "",
        f"  Datasets com mudanças nesta execução: {len(changed)}/{len(INPI_DATASETS)}",
        "=" * 65,
    ]

    report = "\n".join(lines)
    logger.info("\n" + report)
    print(report)


# ============================================================================
# DAG
# ============================================================================

with DAG(
    dag_id="inpi_download",
    default_args=default_args,
    description="Download periódico dos CSVs de patentes do portal de dados abertos do INPI",
    schedule_interval="0 5 * * 1",  # toda segunda-feira às 05:00 UTC
    catchup=False,
    max_active_runs=1,
    params={
        "force": False,  # passar force=True via conf para forçar re-download
    },
    tags=["inpi", "patentes", "download", "csv"],
) as dag:

    # Task 1: verificar se algum arquivo mudou (ShortCircuit)
    check_task = ShortCircuitOperator(
        task_id="check_any_changed",
        python_callable=_check_any_changed,
        provide_context=True,
        ignore_downstream_trigger_rules=True,
    )

    # Tasks 2..N: uma por dataset, rodando em paralelo
    download_tasks = []
    for _dataset_name, _url in INPI_DATASETS.items():
        t = PythonOperator(
            task_id=f"download_{_dataset_name}",
            python_callable=_download_dataset,
            op_kwargs={"dataset_name": _dataset_name, "url": _url},
            provide_context=True,
            execution_timeout=timedelta(minutes=15),
        )
        check_task >> t  # type: ignore
        download_tasks.append(t)

    # Task final: relatório — roda mesmo que alguns downloads tenham sido ignorados
    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for _t in download_tasks:
        _t >> report_task  # type: ignore

    # Aciona a carga no PostgreSQL após download bem-sucedido
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_inpi_load_postgres",
        trigger_dag_id="inpi_load_postgres",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    report_task >> trigger_load  # type: ignore
