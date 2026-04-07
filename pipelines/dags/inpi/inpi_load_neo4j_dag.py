"""
INPI Load Neo4j DAG

Lê os dados de patentes do PostgreSQL (schema inpi) e popula o grafo Neo4j.

Fluxo
─────
ensure_indexes               (CREATE CONSTRAINT/INDEX … IF NOT EXISTS)
        │
        ▼
merge_patentes               (MERGE 145k :Patente nodes)
        │
   ┌────┼────────────────┬────────────────────┐
   ▼    ▼                ▼                    ▼
link_  merge_ipc_         merge_pf_           link_
empresa_ and_classify     and_deposito        vinculos
deposito (ClasseIPC +     (DepositantePF +    (VINCULADA_A)
(DEPOSITOU)  CLASSIFICADA_COMO)  DEPOSITOU)
   └────┴────────────────┴────────────────────┘
        │
        ▼
generate_report              (TriggerRule.ALL_DONE)

Trigger:
  - Automático via TriggerDagRunOperator ao fim do inpi_load_postgres DAG
  - Manual via UI ou CLI (schedule_interval=None)

Parâmetros:
  Nenhum. Sempre lê o estado corrente do PostgreSQL e aplica MERGE idempotente.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.insert(
    0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts")
)

from inpi.neo4j_loader import (
    build_report,
    ensure_inpi_schema,
    link_empresa_deposito,
    link_vinculos,
    merge_ipc_and_classify,
    merge_patentes,
    merge_pf_and_deposito,
)

logger = logging.getLogger(__name__)

# ============================================================================
# Default args
# ============================================================================

default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ============================================================================
# Callables
# ============================================================================


def _ensure_indexes(**context) -> dict:
    return ensure_inpi_schema()


def _merge_patentes(**context) -> dict:
    return merge_patentes()


def _merge_ipc_and_classify(**context) -> dict:
    return merge_ipc_and_classify()


def _merge_pf_and_deposito(**context) -> dict:
    return merge_pf_and_deposito()


def _link_empresa_deposito(**context) -> dict:
    return link_empresa_deposito()


def _link_vinculos(**context) -> dict:
    return link_vinculos()


def _generate_report(**context) -> None:
    ti = context["task_instance"]
    run_id = context["run_id"]

    task_results: dict = {}

    def _pull(task_id: str) -> dict:
        val = ti.xcom_pull(task_ids=task_id, key="return_value")
        return val if isinstance(val, dict) else {}

    task_results.update(_pull("merge_patentes"))
    task_results.update(_pull("merge_ipc_and_classify"))
    task_results.update(_pull("merge_pf_and_deposito"))
    task_results.update(_pull("link_empresa_deposito"))
    task_results.update(_pull("link_vinculos"))

    header = f"  Run ID : {run_id}\n  Data   : {datetime.utcnow().isoformat(timespec='seconds')} UTC"
    report = build_report(task_results)
    full = report.replace("=" * 65, "=" * 65 + "\n" + header, 1)

    logger.info("\n" + full)
    print(full)


# ============================================================================
# DAG
# ============================================================================

with DAG(
    dag_id="inpi_load_neo4j",
    default_args=default_args,
    description="Carga das patentes INPI no grafo Neo4j a partir do PostgreSQL",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["inpi", "patentes", "load", "neo4j"],
) as dag:

    # ── 1. Schema ─────────────────────────────────────────────────────────────
    ensure_task = PythonOperator(
        task_id="ensure_indexes",
        python_callable=_ensure_indexes,
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 2. :Patente nodes ─────────────────────────────────────────────────────
    patentes_task = PythonOperator(
        task_id="merge_patentes",
        python_callable=_merge_patentes,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    # ── 3a. :ClasseIPC nodes + CLASSIFICADA_COMO rels ─────────────────────────
    ipc_task = PythonOperator(
        task_id="merge_ipc_and_classify",
        python_callable=_merge_ipc_and_classify,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    # ── 3b. :DepositantePF nodes + PF DEPOSITOU rels ─────────────────────────
    pf_task = PythonOperator(
        task_id="merge_pf_and_deposito",
        python_callable=_merge_pf_and_deposito,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    # ── 3c. Empresa DEPOSITOU rels (Empresa nodes must already exist) ─────────
    empresa_task = PythonOperator(
        task_id="link_empresa_deposito",
        python_callable=_link_empresa_deposito,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    # ── 3d. VINCULADA_A rels ──────────────────────────────────────────────────
    vinculos_task = PythonOperator(
        task_id="link_vinculos",
        python_callable=_link_vinculos,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    # ── 4. Report ─────────────────────────────────────────────────────────────
    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependências ──────────────────────────────────────────────────────────
    parallel_tasks = [ipc_task, pf_task, empresa_task, vinculos_task]

    ensure_task >> patentes_task  # type: ignore

    for _t in parallel_tasks:
        patentes_task >> _t >> report_task  # type: ignore
