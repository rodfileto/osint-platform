"""
FINEP Load Operação Direta DAG

Lê a aba Projetos_Operação_Direta do Contratacao.xlsx mais recente
e popula a tabela finep.projetos_operacao_direta no PostgreSQL.

Fluxo:
  start
    └─▶ load_operacao_direta
              └─▶ end

Trigger:
  - Automático via TriggerDagRunOperator ao fim da finep_download
  - Manual via UI ou CLI
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts/finep")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _load(**context) -> dict:
    """
    Chama o loader. Aceita manifest_id via conf para permitir re-runs
    direcionados a um download específico.
    """
    from loader_contratacao import load_operacao_direta

    conf = context["dag_run"].conf or {}
    manifest_id = conf.get("manifest_id")           # opcional
    xlsx_path   = conf.get("xlsx_path")             # opcional

    result = load_operacao_direta(
        manifest_id=manifest_id,
        xlsx_path=xlsx_path,
    )

    logger.info(
        f"Carga concluída | "
        f"manifest_id={result['manifest_id']} | "
        f"{result['rows_loaded']:,} linhas | "
        f"{result['duration_seconds']}s"
    )
    return result


with DAG(
    dag_id="finep_load_operacao_direta",
    default_args=default_args,
    description="Carga de Projetos_Operação_Direta (Contratacao.xlsx) → PostgreSQL",
    schedule_interval=None,          # acionada via TriggerDagRunOperator
    catchup=False,
    max_active_runs=1,
    params={
        "manifest_id": None,         # None = usa o download mais recente
        "xlsx_path":   None,         # None = resolve via manifesto
    },
    tags=["finep", "load", "postgres", "contratacao"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_task = PythonOperator(
        task_id="load_operacao_direta",
        python_callable=_load,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    start >> load_task >> end
