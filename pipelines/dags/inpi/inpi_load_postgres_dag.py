"""
INPI Load PostgreSQL DAG

Lê os CSVs de patentes do INPI armazenados no MinIO
e popula as 10 tabelas do schema inpi no PostgreSQL.

Fluxo
─────
check_snapshot_available  (ShortCircuit)
          │
          ▼
   truncate_tables           (TRUNCATE dados_bibliograficos CASCADE)
          │
          ▼
load_dados_bibliograficos   (tabela pai — deve ser carregada PRIMEIRO)
          │
     ┌────┴────────────────────────────────────────────────────┐
     ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼
load_conteudo  load_inventores  load_depositantes  load_classificacao_ipc
load_despachos  load_procuradores  load_prioridades  load_vinculos  load_renumeracoes
     └────┬────────────────────────────────────────────────────┘
          ▼
   mark_all_loaded            (atualiza processing_status='loaded' no manifesto)
          │
          ▼
   refresh_matview             (REFRESH MATERIALIZED VIEW CONCURRENTLY mv_patent_search)
          │
          ▼
   generate_report             (TriggerRule.ALL_DONE — roda mesmo em falha parcial)

Trigger:
  - Automático via TriggerDagRunOperator ao fim do inpi_download DAG
  - Manual via UI ou CLI (schedule_interval=None)

Parâmetros opcionais (via conf):
  force_reload (bool) — ignora verificação de snapshot disponível (default: False)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.python import PythonOperator, ShortCircuitOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.insert(
    0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts")
)

from inpi.loader import (
    check_snapshot_available,
    get_snapshot_info,
    load_classificacao_ipc,
    load_conteudo,
    load_dados_bibliograficos,
    load_depositantes,
    load_despachos,
    load_inventores,
    load_prioridades,
    load_procuradores,
    load_renumeracoes,
    load_vinculos,
    mark_all_loaded,
    refresh_matview,
    truncate_all_tables,
)

logger = logging.getLogger(__name__)

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


# ============================================================================
# Callables
# ============================================================================

def _check_snapshot(**context) -> bool:
    """
    ShortCircuit: retorna True se há datasets com status='downloaded'.
    force_reload=True ignora a verificação e sempre prossegue.
    """
    force_reload = context["dag_run"].conf.get("force_reload", False)
    if force_reload:
        logger.info("force_reload=True — ignorando verificação de snapshot.")
        return True
    return check_snapshot_available()


def _truncate_tables(**context) -> None:
    truncate_all_tables()


def _load_dados_bibliograficos(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_dados_bibliograficos")
    rows = load_dados_bibliograficos(info, info["patentes_dados_bibliograficos"]["snapshot_date"])
    return {"dataset": "patentes_dados_bibliograficos", "rows": rows}


def _load_conteudo(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_conteudo")
    rows = load_conteudo(info, info["patentes_conteudo"]["snapshot_date"])
    return {"dataset": "patentes_conteudo", "rows": rows}


def _load_inventores(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_inventores")
    rows = load_inventores(info, info["patentes_inventores"]["snapshot_date"])
    return {"dataset": "patentes_inventores", "rows": rows}


def _load_depositantes(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_depositantes")
    rows = load_depositantes(info, info["patentes_depositantes"]["snapshot_date"])
    return {"dataset": "patentes_depositantes", "rows": rows}


def _load_classificacao_ipc(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_classificacao_ipc")
    rows = load_classificacao_ipc(info, info["patentes_classificacao_ipc"]["snapshot_date"])
    return {"dataset": "patentes_classificacao_ipc", "rows": rows}


def _load_despachos(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_despachos")
    rows = load_despachos(info, info["patentes_despachos"]["snapshot_date"])
    return {"dataset": "patentes_despachos", "rows": rows}


def _load_procuradores(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_procuradores")
    rows = load_procuradores(info, info["patentes_procuradores"]["snapshot_date"])
    return {"dataset": "patentes_procuradores", "rows": rows}


def _load_prioridades(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_prioridades")
    rows = load_prioridades(info, info["patentes_prioridades"]["snapshot_date"])
    return {"dataset": "patentes_prioridades", "rows": rows}


def _load_vinculos(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_vinculos")
    rows = load_vinculos(info, info["patentes_vinculos"]["snapshot_date"])
    return {"dataset": "patentes_vinculos", "rows": rows}


def _load_renumeracoes(**context) -> dict:
    info = get_snapshot_info()
    _validate_snapshot(info, "patentes_renumeracoes")
    rows = load_renumeracoes(info, info["patentes_renumeracoes"]["snapshot_date"])
    return {"dataset": "patentes_renumeracoes", "rows": rows}


def _mark_all_loaded(**context) -> None:
    ti = context["task_instance"]
    info = get_snapshot_info()

    task_map = {
        "patentes_dados_bibliograficos": "load_dados_bibliograficos",
        "patentes_conteudo":             "load_conteudo",
        "patentes_inventores":           "load_inventores",
        "patentes_depositantes":         "load_depositantes",
        "patentes_classificacao_ipc":    "load_classificacao_ipc",
        "patentes_despachos":            "load_despachos",
        "patentes_procuradores":         "load_procuradores",
        "patentes_prioridades":          "load_prioridades",
        "patentes_vinculos":             "load_vinculos",
        "patentes_renumeracoes":         "load_renumeracoes",
    }
    rows_per_dataset: dict[str, int] = {}
    for dataset, task_id in task_map.items():
        result = ti.xcom_pull(task_ids=task_id, key="return_value") or {}
        rows_per_dataset[dataset] = result.get("rows", 0) if isinstance(result, dict) else 0

    mark_all_loaded(info, rows_per_dataset)


def _refresh_matview(**context) -> None:
    refresh_matview()


def _generate_report(**context) -> None:
    """Imprime sumário consolidado da carga."""
    ti = context["task_instance"]
    run_id = context["run_id"]

    datasets = [
        "patentes_dados_bibliograficos",
        "patentes_conteudo",
        "patentes_inventores",
        "patentes_depositantes",
        "patentes_classificacao_ipc",
        "patentes_despachos",
        "patentes_procuradores",
        "patentes_prioridades",
        "patentes_vinculos",
        "patentes_renumeracoes",
    ]
    task_ids = [
        "load_dados_bibliograficos",
        "load_conteudo",
        "load_inventores",
        "load_depositantes",
        "load_classificacao_ipc",
        "load_despachos",
        "load_procuradores",
        "load_prioridades",
        "load_vinculos",
        "load_renumeracoes",
    ]

    lines = [
        "=" * 65,
        "  INPI Patentes — PostgreSQL Load Report",
        f"  Run ID : {run_id}",
        f"  Data   : {datetime.utcnow().isoformat(timespec='seconds')} UTC",
        "=" * 65,
    ]

    total_rows = 0
    for dataset, task_id in zip(datasets, task_ids):
        result = ti.xcom_pull(task_ids=task_id, key="return_value")
        if isinstance(result, dict) and "rows" in result:
            rows = result["rows"]
            total_rows += rows
            lines.append(f"  ✓  {dataset:<45} {rows:>10,} linhas")
        else:
            lines.append(f"  ✗  {dataset:<45} {'FALHOU ou ignorado':>15}")

    lines += [
        "-" * 65,
        f"  Total                                                    {total_rows:>10,} linhas",
        "=" * 65,
    ]

    report = "\n".join(lines)
    logger.info("\n" + report)
    print(report)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_snapshot(info: dict, dataset: str) -> None:
    if dataset not in info:
        raise RuntimeError(
            f"Dataset '{dataset}' não encontrado no manifesto INPI "
            f"com status='downloaded'. Verifique o inpi_download DAG."
        )


# ============================================================================
# DAG
# ============================================================================

CHILD_TABLES = [
    ("load_conteudo",          _load_conteudo,          "patentes_conteudo"),
    ("load_inventores",        _load_inventores,         "patentes_inventores"),
    ("load_depositantes",      _load_depositantes,       "patentes_depositantes"),
    ("load_classificacao_ipc", _load_classificacao_ipc,  "patentes_classificacao_ipc"),
    ("load_despachos",         _load_despachos,          "patentes_despachos"),
    ("load_procuradores",      _load_procuradores,       "patentes_procuradores"),
    ("load_prioridades",       _load_prioridades,        "patentes_prioridades"),
    ("load_vinculos",          _load_vinculos,           "patentes_vinculos"),
    ("load_renumeracoes",      _load_renumeracoes,       "patentes_renumeracoes"),
]

with DAG(
    dag_id="inpi_load_postgres",
    default_args=default_args,
    description="Carga das 10 tabelas INPI Patentes no PostgreSQL a partir de snapshots no MinIO",
    schedule_interval=None,     # acionado via TriggerDagRunOperator ou manualmente
    catchup=False,
    max_active_runs=1,
    params={
        "force_reload": False,  # True → ignora verificação de snapshot disponível
    },
    tags=["inpi", "patentes", "load", "postgres"],
) as dag:

    # ── 1. ShortCircuit: verifica se há snapshot disponível ──────────────────
    check_task = ShortCircuitOperator(
        task_id="check_snapshot_available",
        python_callable=_check_snapshot,
        provide_context=True,
        ignore_downstream_trigger_rules=True,
    )

    # ── 2. TRUNCATE todas as tabelas (cascata via FK) ─────────────────────────
    truncate_task = PythonOperator(
        task_id="truncate_tables",
        python_callable=_truncate_tables,
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 3. Carga da tabela pai ────────────────────────────────────────────────
    load_bib_task = PythonOperator(
        task_id="load_dados_bibliograficos",
        python_callable=_load_dados_bibliograficos,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    # ── 4. Carga das 9 tabelas filhas (em paralelo após o pai) ───────────────
    child_load_tasks = []
    for _task_id, _callable, _dataset in CHILD_TABLES:
        t = PythonOperator(
            task_id=_task_id,
            python_callable=_callable,
            provide_context=True,
            execution_timeout=timedelta(hours=1),
        )
        child_load_tasks.append(t)

    # ── 5. Marcar manifests como 'loaded' (após todos os filhos) ─────────────
    mark_loaded_task = PythonOperator(
        task_id="mark_all_loaded",
        python_callable=_mark_all_loaded,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 6. Refresh da materialized view ──────────────────────────────────────
    refresh_task = PythonOperator(
        task_id="refresh_matview",
        python_callable=_refresh_matview,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    # ── 7. Relatório final ────────────────────────────────────────────────────
    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependências ──────────────────────────────────────────────────────────
    check_task >> truncate_task >> load_bib_task  # type: ignore

    for _child_task in child_load_tasks:
        load_bib_task >> _child_task >> mark_loaded_task  # type: ignore

    mark_loaded_task >> refresh_task >> report_task  # type: ignore

    # ── 8. Acionar carga no Neo4j ─────────────────────────────────────────────
    trigger_neo4j = TriggerDagRunOperator(
        task_id="trigger_neo4j_load",
        trigger_dag_id="inpi_load_neo4j",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    report_task >> trigger_neo4j  # type: ignore
