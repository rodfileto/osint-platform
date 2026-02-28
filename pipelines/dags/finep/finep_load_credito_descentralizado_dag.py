"""
FINEP Load Crédito Descentralizado DAG

Lê a aba Proj__Crédito_Descentralizado do Contratacao.xlsx mais recente
e popula a tabela finep.projetos_credito_descentralizado no PostgreSQL.

Estrutura: 3.035 linhas, 10 colunas.
Cada linha é uma sub-operação de crédito intermediada por um banco
(agente financeiro) com recursos do BNDES / FINEP.

Fluxo:
  start ──▶ load_credito_descentralizado ──▶ end

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
    from loader_credito_descentralizado import load_credito_descentralizado

    conf = context["dag_run"].conf or {}
    manifest_id = conf.get("manifest_id")
    xlsx_path   = conf.get("xlsx_path")

    result = load_credito_descentralizado(
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
    dag_id="finep_load_credito_descentralizado",
    default_args=default_args,
    description="Carga de Proj__Crédito_Descentralizado (Contratacao.xlsx) → PostgreSQL",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={
        "manifest_id": None,
        "xlsx_path":   None,
    },
    tags=["finep", "load", "postgres", "contratacao"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_task = PythonOperator(
        task_id="load_credito_descentralizado",
        python_callable=_load,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    start >> load_task >> end
