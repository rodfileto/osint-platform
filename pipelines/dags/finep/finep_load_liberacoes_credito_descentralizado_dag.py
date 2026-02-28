"""
FINEP Load Liberações Crédito Descentralizado DAG

Lê a aba Proj__Crédito_Descentralizado do Liberacao.xlsx e popula
finep.liberacoes_credito_descentralizado (~4.700 linhas).

Apenas campos delta: contrato, cnpj_proponente, num_parcela,
data_liberacao, valor_liberado.

Fluxo:
  start ──▶ load_liberacoes_credito_descentralizado ──▶ end

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
    from loader_liberacoes_credito_descentralizado import load_liberacoes_credito_descentralizado

    conf = context["dag_run"].conf or {}
    result = load_liberacoes_credito_descentralizado(
        manifest_id=conf.get("manifest_id"),
        xlsx_path=conf.get("xlsx_path"),
    )
    logger.info(
        f"Carga concluída | manifest_id={result['manifest_id']} | "
        f"{result['rows_loaded']:,} linhas | {result['duration_seconds']}s"
    )
    return result


with DAG(
    dag_id="finep_load_liberacoes_credito_descentralizado",
    default_args=default_args,
    description="Carga de liberações Crédito Descentralizado (Liberacao.xlsx) → PostgreSQL",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={"manifest_id": None, "xlsx_path": None},
    tags=["finep", "load", "postgres", "liberacao"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_task = PythonOperator(
        task_id="load_liberacoes_credito_descentralizado",
        python_callable=_load,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    start >> load_task >> end
