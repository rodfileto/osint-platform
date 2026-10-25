"""
FINEP pipeline: download the two FINEP spreadsheets, then load the seven finep.* tables.

A spreadsheet is downloaded only when FINEP publishes a new version (ETag/Last-Modified), and a
table is reloaded only when it does not already hold the newest spreadsheet. Reloading
projetos_operacao_direta removes its NLP topic assignments; re-run finep_nlp_topic afterwards.

Manual runs:
    airflow dags trigger finep_pipeline --conf '{"force_download": true}'
    airflow dags trigger finep_pipeline --conf '{"force_reload": true}'
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

SCRIPTS_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts")
sys.path.insert(0, SCRIPTS_DIR)

from finep.downloader import FINEP_DATASETS  # noqa: E402
from finep.loader import TABLES  # noqa: E402

OUTPUT_DIR = Path(os.environ.get("FINEP_OUTPUT_DIR", "/opt/airflow/data/finep"))

with DAG(
    dag_id="finep_pipeline",
    description="Download FINEP spreadsheets and load the finep schema",
    schedule="0 6 2 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "osint-platform", "retries": 2, "retry_delay": timedelta(minutes=10)},
    params={
        "force_download": Param(False, type="boolean", description="Download even if FINEP reports no change"),
        "force_reload": Param(False, type="boolean", description="Reload tables that already hold the newest file"),
    },
    tags=["finep", "etl"],
) as dag:

    @task(execution_timeout=timedelta(hours=1))
    def download(dataset_type: str, params=None):
        from finep.downloader import collect_dataset

        return collect_dataset(dataset_type, FINEP_DATASETS[dataset_type], OUTPUT_DIR, force=params["force_download"])

    @task(execution_timeout=timedelta(hours=1))
    def load(table: str, params=None):
        from finep.loader import load_table

        return load_table(table, force=params["force_reload"])

    downloads = {name: download.override(task_id=f"download_{name}")(name) for name in FINEP_DATASETS}
    for table, spec in TABLES.items():
        downloads[spec.dataset] >> load.override(task_id=f"load_{table}")(table)
